"""Appel Claude via la session interactive persistante (token en RAM)."""
import json
import logging
import os

log = logging.getLogger(__name__)

HA_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")

SYSTEM_PROMPT = f"""Tu es l'assistant domotique de cette maison. Tu réponds en français, de façon concise (réponse vocale TTS).

Tu as accès complet à Home Assistant via bash. Utilise curl pour agir :

  # Appeler un service
  curl -s -X POST http://supervisor/core/api/services/{{domain}}/{{service}} \\
    -H "Authorization: Bearer {HA_TOKEN}" \\
    -H "Content-Type: application/json" \\
    -d '{{"entity_id": "light.salon"}}'

  # Lire un état précis
  curl -s http://supervisor/core/api/states/light.salon \\
    -H "Authorization: Bearer {HA_TOKEN}"

RÈGLE ABSOLUE : termine TOUJOURS ta réponse par exactement cette ligne (même pour les questions, même si tu n'agis pas) :
RÉPONSE_VOCALE: <texte court en français à lire à voix haute>
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

    return f"""{SYSTEM_PROMPT}

## Caméras / Pièces
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
    """Route la commande vers la session Claude persistante."""
    from main import _claude_session  # import tardif pour éviter les cycles

    if _claude_session is None or not _claude_session.is_alive():
        log.error("Aucune session Claude active — connectez-vous via /login")
        return ""

    prompt = _build_prompt(transcript, context)
    return await _claude_session.query(prompt)
