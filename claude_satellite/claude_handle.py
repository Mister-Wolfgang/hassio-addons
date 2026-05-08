"""Appel Claude via la session interactive persistante (token en RAM)."""
import logging
import os

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
        for cam, score in context.get("ww_scores", {}).items()
    )

    return f"""{SYSTEM_PROMPT}

## Caméras / Pièces
{cameras_lines}

## Niveau audio au wake word (rms élevé = personne proche)
{scores_lines}

---
Commande vocale : "{transcript}"
"""


async def handle(transcript: str, context: dict, **_) -> str:
    """Route la commande vers la session Claude persistante."""
    from main import _claude_session  # import tardif pour éviter les cycles

    if _claude_session is None or not _claude_session.is_alive():
        log.error("Aucune session Claude active — connectez-vous via /login")
        return ""

    prompt = _build_prompt(transcript, context)
    return await _claude_session.query(prompt)
