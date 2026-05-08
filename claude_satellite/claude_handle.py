"""Appel Claude via le bridge HTTP sur iaserver (claude CLI compte Max)."""
import json
import logging
import os

import httpx

log = logging.getLogger(__name__)

HA_URL = "http://supervisor/core"
HA_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")

SYSTEM_PROMPT = f"""Tu es l'assistant domotique de cette maison. Tu réponds en français, de façon concise (réponse vocale TTS).

Pour contrôler Home Assistant, utilise l'outil bash avec curl :
  # Appeler un service
  curl -s -X POST {HA_URL}/api/services/{{domain}}/{{service}} \\
    -H "Authorization: Bearer {HA_TOKEN}" \\
    -H "Content-Type: application/json" \\
    -d '{{"entity_id": "light.salon"}}'

  # Lire un état
  curl -s {HA_URL}/api/states/{{entity_id}} \\
    -H "Authorization: Bearer {HA_TOKEN}"

Après avoir agi, termine ta réponse par exactement cette ligne :
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

    return f"""## Caméras disponibles
{cameras_lines}

## Signaux audio au moment du wake word
(score élevé + rms élevé = personne proche de cette caméra)
{scores_lines}

## Événements Frigate (30 dernières secondes)
{events_json}

## États Home Assistant
{states_lines}

---
Commande vocale reçue : "{transcript}"

Raisonne sur les signaux pour identifier qui a parlé et depuis quelle pièce, puis réponds et agis.
"""


async def handle(transcript: str, context: dict, bridge_url: str) -> str:
    """Envoie le prompt au bridge Claude sur iaserver, retourne le texte TTS."""
    prompt = _build_prompt(transcript, context)

    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(
                f"{bridge_url}/ask",
                json={"system": SYSTEM_PROMPT, "prompt": prompt},
                timeout=90,
            )
            r.raise_for_status()
            data = r.json()
            tts = data.get("tts", "")
            log.info("Réponse Claude: %r", tts)
            return tts
    except Exception as e:
        log.error("Bridge error: %s", e)
        return ""
