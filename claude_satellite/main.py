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


async def _get_frigate_rtsp_base(ha_headers: dict) -> str | None:
    """Trouve l'URL RTSP go2rtc depuis la config de l'intégration Frigate dans HA."""
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(
                "http://supervisor/core/api/config/config_entries/entry",
                headers=ha_headers,
                timeout=10,
            )
            r.raise_for_status()
            entries = r.json()
    except Exception as e:
        log.warning("Impossible de lire les config_entries HA: %s", e)
        return None

    for entry in entries:
        if entry.get("domain") != "frigate":
            continue
        title = entry.get("title", "")
        # title = "192.168.1.131:5000" → on extrait l'hôte
        host = title.split(":")[0]
        if host:
            rtsp_base = f"rtsp://{host}:8554"
            log.info("Frigate détecté via config_entry: %s → RTSP base: %s", title, rtsp_base)
            return rtsp_base

    return None


async def _discover_cameras(go2rtc_rtsp: str) -> list[CameraConfig]:
    """Découvre automatiquement les caméras Frigate depuis les states HA."""
    ha_token = os.environ.get("SUPERVISOR_TOKEN", "")
    headers = {"Authorization": f"Bearer {ha_token}"}

    # Auto-découverte de l'URL RTSP si non configurée explicitement
    rtsp_base = go2rtc_rtsp
    if not go2rtc_rtsp or go2rtc_rtsp == "rtsp://homeassistant:8554":
        discovered = await _get_frigate_rtsp_base(headers)
        if discovered:
            rtsp_base = discovered

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
        if attrs.get("client_id") != "frigate":
            continue

        camera_name = attrs.get("camera_name") or entity_id.removeprefix("camera.")
        friendly_name = attrs.get("friendly_name", camera_name)
        rtsp_url = f"{rtsp_base}/{camera_name}"

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
        cameras = []
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
        log.info("Auto-découverte des caméras Frigate via l'API HA...")
        cameras = await _discover_cameras(go2rtc_rtsp)
        if cameras:
            # Déduire l'URL Frigate HTTP depuis la même IP que go2rtc
            rtsp_host = go2rtc_rtsp.replace("rtsp://", "").split(":")[0]
            if rtsp_host not in ("homeassistant", "localhost"):
                config.setdefault("frigate_url", f"http://{rtsp_host}:5000")
                log.info("Frigate URL: %s", config["frigate_url"])
        else:
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
