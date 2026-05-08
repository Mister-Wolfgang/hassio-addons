"""Appel Claude via claude -p (mode non-interactif, credentials depuis keyring)."""
import asyncio
import logging
import os
import re

log = logging.getLogger(__name__)

HA_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")

SYSTEM_PROMPT = f"""Tu es l'assistant domotique de cette maison. Tu réponds en français, de façon concise (réponse vocale TTS).

Tu as accès complet à Home Assistant via bash. Utilise curl pour agir :

  curl -s http://supervisor/core/api/states \\
    -H "Authorization: Bearer {HA_TOKEN}"

  curl -s -X POST http://supervisor/core/api/services/{{domain}}/{{service}} \\
    -H "Authorization: Bearer {HA_TOKEN}" \\
    -H "Content-Type: application/json" \\
    -d '{{"entity_id": "light.salon"}}'

RÈGLE ABSOLUE : termine TOUJOURS ta réponse par exactement cette ligne :
RÉPONSE_VOCALE: <texte court en français à lire à voix haute>
"""


def _build_prompt(transcript: str, context: dict) -> str:
    cameras_lines = "\n".join(
        f"  - {c['name']} → pièce: {c['room']}" for c in context.get("cameras", [])
    )
    scores_lines = "\n".join(
        f"  - {cam}: rms={context['rms_values'].get(cam, 0):.3f}"
        for cam in context.get("ww_scores", {})
    )
    return (
        f"{SYSTEM_PROMPT}\n"
        f"## Caméras / Pièces\n{cameras_lines}\n\n"
        f"## Niveau audio (rms élevé = personne proche)\n{scores_lines}\n\n"
        f"---\nCommande vocale : \"{transcript}\"\n"
    )


async def handle(transcript: str, context: dict, **_) -> str:
    """Lance claude -p avec le prompt et retourne le texte RÉPONSE_VOCALE."""
    prompt = _build_prompt(transcript, context)
    env = {**os.environ, "HOME": "/data"}
    try:
        proc = await asyncio.create_subprocess_exec(
            "claude", "-p", prompt,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=90.0)
        except asyncio.TimeoutError:
            proc.kill()
            log.warning("claude -p timeout")
            return ""

        if proc.returncode != 0:
            log.error("claude -p exit %d: %s", proc.returncode,
                      stderr.decode("utf-8", errors="replace")[:300])
            return ""

        output = stdout.decode("utf-8", errors="replace")
        log.debug("claude -p output: %r", output[-400:])
        m = re.search(r"RÉPONSE_VOCALE\s*:\s*(.+)", output, re.IGNORECASE)
        if m:
            log.info("Claude réponse: %r", m.group(1).strip())
            return m.group(1).strip()
        log.warning("claude -p: pas de RÉPONSE_VOCALE — output: %r", output[-200:])
        return ""
    except Exception as e:
        log.error("claude -p error: %s", e)
        return ""
