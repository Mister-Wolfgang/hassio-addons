"""Entry point FastAPI — health check + démarrage du satellite."""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from satellite import MultiMicSatellite, CameraConfig

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

OPTIONS_PATH = os.environ.get("OPTIONS_PATH", "/data/options.json")
_satellite: MultiMicSatellite | None = None


def _load_config() -> dict:
    with open(OPTIONS_PATH) as f:
        return json.load(f)


async def _discover_cameras(go2rtc_rtsp: str) -> list[CameraConfig]:
    """Découvre automatiquement les caméras Frigate depuis les states HA."""
    ha_token = os.environ.get("SUPERVISOR_TOKEN", "")
    headers = {"Authorization": f"Bearer {ha_token}"}

    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(
                "http://supervisor/core/api/states",
                headers=headers,
                timeout=10,
            )
            r.raise_for_status()
            states = r.json()
    except Exception as e:
        log.error("Impossible de contacter l'API HA pour la découverte: %s", e)
        return []

    cameras = []
    for state in states:
        entity_id = state.get("entity_id", "")
        if not entity_id.startswith("camera."):
            continue
        attrs = state.get("attributes", {})
        # Uniquement les caméras Frigate
        if attrs.get("client_id") != "frigate":
            continue

        camera_name = attrs.get("camera_name") or entity_id.removeprefix("camera.")
        friendly_name = attrs.get("friendly_name", camera_name)
        rtsp_url = f"{go2rtc_rtsp}/{camera_name}"

        cameras.append(CameraConfig(
            name=camera_name,
            room=friendly_name,
            rtsp_url=rtsp_url,
            frigate_camera=camera_name,
            talkback_camera=camera_name,
        ))
        log.info("Caméra découverte: %s (%s) -> %s", camera_name, friendly_name, rtsp_url)

    return cameras


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _satellite

    config = _load_config()
    go2rtc_rtsp = config.get("go2rtc_rtsp", "rtsp://homeassistant:8554")

    manual_cameras = config.get("cameras", [])
    if manual_cameras:
        # Caméras déclarées manuellement — on les utilise telles quelles
        cameras = []
        go2rtc_url = config.get("go2rtc_url", "http://homeassistant:1984")
        for c in manual_cameras:
            rtsp_url = c.get("rtsp_url") or f"{go2rtc_rtsp}/{c['frigate_camera']}"
            cameras.append(CameraConfig(
                name=c["name"],
                room=c.get("room", c["name"]),
                rtsp_url=rtsp_url,
                frigate_camera=c["frigate_camera"],
                talkback_camera=c.get("talkback_camera", c["frigate_camera"]),
            ))
        log.info("%d caméra(s) chargée(s) depuis la config", len(cameras))
    else:
        # Auto-découverte via l'API HA
        log.info("Aucune caméra configurée — découverte automatique via HA...")
        cameras = await _discover_cameras(go2rtc_rtsp)
        if not cameras:
            log.warning("Aucune caméra Frigate trouvée — satellite inactif")

    _satellite = MultiMicSatellite(cameras=cameras, config=config)
    task = asyncio.create_task(_satellite.start())

    log.info("Claude Satellite démarré — %d caméra(s)", len(cameras))
    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Claude Satellite", lifespan=lifespan)


@app.get("/health")
async def health():
    n = len(_satellite.cameras) if _satellite else 0
    return JSONResponse({"status": "ok", "cameras": n})


@app.get("/")
async def index():
    if not _satellite:
        return JSONResponse({"status": "no satellite"})
    return JSONResponse({
        "status": "running",
        "cameras": [
            {"name": s.camera.name, "room": s.camera.room, "rms": round(s.rms, 4)}
            for s in _satellite.streams.values()
        ],
    })
