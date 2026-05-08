"""Appel Claude avec contexte complet + tool use HA."""
import json
import logging
import os

import anthropic
import httpx

log = logging.getLogger(__name__)

HA_URL = "http://supervisor/core"
HA_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
_HA_HEADERS = {"Authorization": f"Bearer {HA_TOKEN}"}

HA_TOOLS = [
    {
        "name": "call_ha_service",
        "description": "Appelle un service Home Assistant pour contrôler un appareil (lumière, chauffage, volet, média...)",
        "input_schema": {
            "type": "object",
            "properties": {
                "domain":    {"type": "string", "description": "Domaine HA : light, switch, climate, cover, media_player, script..."},
                "service":   {"type": "string", "description": "Service : turn_on, turn_off, toggle, set_temperature, set_volume_level..."},
                "entity_id": {"type": "string", "description": "ID de l'entité (ex: light.salon_principal)"},
                "data":      {"type": "object", "description": "Paramètres optionnels (brightness, temperature, volume_level...)"},
            },
            "required": ["domain", "service"],
        },
    },
    {
        "name": "get_ha_state",
        "description": "Lit l'état actuel d'une entité Home Assistant",
        "input_schema": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string"},
            },
            "required": ["entity_id"],
        },
    },
]


async def _call_ha_service(domain: str, service: str, entity_id: str | None = None, data: dict | None = None) -> str:
    payload = dict(data or {})
    if entity_id:
        payload["entity_id"] = entity_id
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{HA_URL}/api/services/{domain}/{service}", headers=_HA_HEADERS, json=payload, timeout=10)
        return "OK" if r.status_code in (200, 201) else f"Erreur {r.status_code}: {r.text[:100]}"


async def _get_ha_state(entity_id: str) -> str:
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{HA_URL}/api/states/{entity_id}", headers=_HA_HEADERS, timeout=5)
        if r.status_code == 200:
            s = r.json()
            attrs = {k: v for k, v in s.get("attributes", {}).items() if k in ("friendly_name", "brightness", "temperature", "current_temperature", "volume_level")}
            return f"{entity_id} = {s['state']} {json.dumps(attrs, ensure_ascii=False)}"
        return f"Entité introuvable: {entity_id}"


async def _execute_tool(name: str, inputs: dict) -> str:
    if name == "call_ha_service":
        return await _call_ha_service(**inputs)
    if name == "get_ha_state":
        return await _get_ha_state(**inputs)
    return f"Outil inconnu: {name}"


def _build_system(context: dict) -> str:
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

    return f"""Tu es l'assistant domotique de cette maison. Tu réponds en français, de façon naturelle et concise (réponse vocale via TTS).

## Caméras disponibles
{cameras_lines}

## Signaux audio au moment du wake word
(wake_word_score = confiance détection, rms = niveau sonore 0..1)
{scores_lines}

Utilise ces deux signaux pour déterminer de quelle caméra la personne est la plus proche, donc dans quelle pièce elle se trouve.
Si plusieurs caméras sont dans la même pièce, la combinaison score×rms la plus élevée indique la plus proche.

## Événements Frigate (30 dernières secondes)
Utilise les sub_label (visages reconnus) et la taille des bounding boxes (plus grande = plus proche) pour identifier qui a parlé.
{events_json}

## États Home Assistant
{states_lines}

## Règles
- Raisonne d'abord silencieusement sur qui a parlé et depuis où
- Réponds directement à la demande
- Utilise les tools pour agir si nécessaire
- Sois bref (max 2 phrases pour la réponse vocale)
"""


def _build_messages(transcript: str, context: dict) -> list[dict]:
    content = []

    for cam_name, b64 in context.get("snapshots", {}).items():
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": "image/jpeg", "data": b64},
        })
        content.append({"type": "text", "text": f"[Snapshot: {cam_name}]"})

    content.append({"type": "text", "text": f'Commande vocale reçue : "{transcript}"'})
    return [{"role": "user", "content": content}]


async def handle(transcript: str, context: dict) -> str:
    """Pipeline Claude avec tool use — retourne le texte à synthétiser."""
    client = anthropic.AsyncAnthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

    system = _build_system(context)
    messages = _build_messages(transcript, context)

    while True:
        response = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            system=system,
            tools=HA_TOOLS,
            messages=messages,
        )

        text_parts = []
        tool_uses = []
        for block in response.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_uses.append(block)

        if not tool_uses:
            return " ".join(text_parts).strip()

        log.info("Claude tool calls: %s", [t.name for t in tool_uses])
        messages.append({"role": "assistant", "content": response.content})

        tool_results = []
        for tu in tool_uses:
            result = await _execute_tool(tu.name, tu.input)
            log.info("  %s(%s) → %s", tu.name, tu.input, result)
            tool_results.append({"type": "tool_result", "tool_use_id": tu.id, "content": result})

        messages.append({"role": "user", "content": tool_results})
