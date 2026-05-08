"""Appel Claude Code CLI (compte Max) directement dans le container addon."""
import asyncio
import json
import logging
import os
import re

log = logging.getLogger(__name__)

HA_URL = "http://supervisor/core"
HA_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
CLAUDE_BIN = "/usr/local/bin/claude"

SYSTEM_PROMPT = f"""Tu es l'assistant domotique de cette maison. Tu réponds en français, de façon concise (réponse vocale TTS).

Tu as accès complet à Home Assistant via bash. Utilise curl pour agir :

  # Lire tous les états
  curl -s http://supervisor/core/api/states \\
    -H "Authorization: Bearer {HA_TOKEN}"

  # Appeler un service
  curl -s -X POST http://supervisor/core/api/services/{{domain}}/{{service}} \\
    -H "Authorization: Bearer {HA_TOKEN}" \\
    -H "Content-Type: application/json" \\
    -d '{{"entity_id": "light.salon"}}'

  # Lire un état précis
  curl -s http://supervisor/core/api/states/light.salon \\
    -H "Authorization: Bearer {HA_TOKEN}"

Après avoir agi, termine TOUJOURS par :
RÉPONSE_VOCALE: <texte court à lire à voix haute>
"""


def _build_prompt(transcript: str, context: dict) -> str:
    cameras_lines = "\n".join(
        f"  - {c['name']} → pièce: {c['room']}" for c in context.get("cameras", [])
    )
    scores_lines = "\n".join(
        f"  - {cam}: wake_word_score={score:.2f}  rms={context['rms_values'].get(cam, 0):.3f}"
        for cam, score in context.get("ww_scores", {}).items()
    )
    events_json = json.dumps(context.get("frigate_events", []), ensure_ascii=False, indent=2)
    states_lines = "\n".join(
        f"  {s['entity_id']}: {s['state']}"
        for s in context.get("ha_states", [])
    )

    return f"""## Caméras / Pièces
{cameras_lines}

## Signaux audio au moment du wake word
(score élevé + rms élevé = personne proche de cette caméra)
{scores_lines}

## Événements Frigate (30 dernières secondes)
{events_json}

## États Home Assistant actuels
{states_lines}

---
Commande vocale reçue : "{transcript}"

Identifie qui a parlé et depuis quelle pièce, puis réponds et agis sur HA si nécessaire.
"""


async def handle(transcript: str, context: dict, **_) -> str:
    """Appelle claude CLI dans le container, retourne le texte TTS."""
    prompt = _build_prompt(transcript, context)

    # Debug: vérifier où sont les credentials
    import pathlib
    for p in ("/data/.claude", "/data/.claude.json", "/root/.claude", "/root/.claude.json"):
        pp = pathlib.Path(p)
        if pp.exists():
            if pp.is_dir():
                files = [str(f.name) for f in pp.iterdir()]
                log.info("credentials dir %s: %s", p, files)
            else:
                log.info("credentials file %s: %d bytes", p, pp.stat().st_size)
        else:
            log.info("credentials path missing: %s", p)

    env = {
        **os.environ,
        "HOME": "/data",  # credentials dans /data/.claude/ — persiste entre redémarrages
    }

    try:
        proc = await asyncio.create_subprocess_exec(
            CLAUDE_BIN,
            "--output-format", "text",
            "--system-prompt", SYSTEM_PROMPT,
            "--allowedTools", "bash",
            "-p", prompt,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
    except asyncio.TimeoutError:
        proc.kill()
        log.error("claude timeout")
        return ""
    except Exception as e:
        log.error("claude exec error: %s", e)
        return ""

    if proc.returncode != 0:
        log.error("claude exit %d\nSTDERR: %s\nSTDOUT: %s",
                  proc.returncode, stderr.decode()[:500], stdout.decode()[:500])
        return ""

    output = stdout.decode()
    log.debug("claude output: %s", output[:500])

    match = re.search(r"RÉPONSE_VOCALE\s*:\s*(.+)", output, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    lines = [l.strip() for l in output.splitlines() if l.strip()]
    return lines[-1] if lines else ""
