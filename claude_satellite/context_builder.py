"""Construit le contexte Frigate + HA à injecter dans Claude."""
import asyncio
import base64
import logging
import os
import time

import httpx

log = logging.getLogger(__name__)

HA_URL = "http://supervisor/core"
HA_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")

_HA_HEADERS = {"Authorization": f"Bearer {HA_TOKEN}"}
_RELEVANT_DOMAINS = ("light.", "switch.", "climate.", "cover.", "media_player.", "person.", "input_boolean.", "sensor.")


async def _get_ha_states() -> list[dict]:
    async with httpx.AsyncClient() as c:
        try:
            r = await c.get(f"{HA_URL}/api/states", headers=_HA_HEADERS, timeout=5)
            states = r.json()
            return [
                {"entity_id": s["entity_id"], "state": s["state"], "attributes": s.get("attributes", {})}
                for s in states
                if s["entity_id"].startswith(_RELEVANT_DOMAINS)
            ]
        except Exception as e:
            log.warning("HA states error: %s", e)
            return []


async def _get_frigate_events(frigate_url: str, cameras: list[str], since_s: float = 30) -> list[dict]:
    after = int(time.time() - since_s)
    params = {"after": after, "limit": 30}
    if cameras:
        params["cameras"] = ",".join(cameras)
    async with httpx.AsyncClient() as c:
        try:
            r = await c.get(f"{frigate_url}/api/events", params=params, timeout=5)
            return r.json() if r.status_code == 200 else []
        except Exception as e:
            log.warning("Frigate events error: %s", e)
            return []


async def _get_snapshot_b64(frigate_url: str, camera: str) -> str | None:
    async with httpx.AsyncClient() as c:
        try:
            r = await c.get(f"{frigate_url}/api/{camera}/latest.jpg", params={"bbox": 1}, timeout=5)
            if r.status_code == 200:
                return base64.standard_b64encode(r.content).decode()
        except Exception as e:
            log.warning("Frigate snapshot error [%s]: %s", camera, e)
    return None


async def build_context(
    frigate_url: str,
    all_cameras: list[dict],
    ww_scores: dict[str, float],
    rms_values: dict[str, float],
) -> dict:
    """Récupère en parallèle Frigate + HA et retourne un dict de contexte."""
    frigate_cams = [c["frigate_camera"] for c in all_cameras]

    events_task = asyncio.create_task(_get_frigate_events(frigate_url, frigate_cams))
    states_task = asyncio.create_task(_get_ha_states())
    snapshot_tasks = {
        c["name"]: asyncio.create_task(_get_snapshot_b64(frigate_url, c["frigate_camera"]))
        for c in all_cameras
    }

    events = await events_task
    states = await states_task
    snapshots = {name: await task for name, task in snapshot_tasks.items()}

    return {
        "ww_scores": ww_scores,
        "rms_values": rms_values,
        "cameras": [{"name": c["name"], "room": c["room"]} for c in all_cameras],
        "frigate_events": events,
        "snapshots": {k: v for k, v in snapshots.items() if v},
        "ha_states": states,
    }
