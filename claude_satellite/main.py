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


async def _resolve_rtsp(camera: dict, go2rtc_url: str, go2rtc_rtsp: str) -> str:
    """Retourne l'URL RTSP — depuis la config si présente, sinon depuis go2rtc."""
    if camera.get("rtsp_url"):
        return camera["rtsp_url"]

    stream_name = camera["frigate_camera"]
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{go2rtc_url}/api/streams", timeout=5)
            streams = r.json()
            if stream_name in streams:
                url = f"{go2rtc_rtsp}/{stream_name}"
                log.info("[%s] RTSP auto-découvert : %s", camera["name"], url)
                return url
    except Exception as e:
        log.warning("[%s] go2rtc unavailable: %s", camera["name"], e)

    raise RuntimeError(f"Impossible de trouver l'URL RTSP pour '{stream_name}'. Configurez rtsp_url manuellement.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _satellite

    config = _load_config()
    go2rtc_url = config.get("go2rtc_url", "http://homeassistant:1984")
    go2rtc_rtsp = config.get("go2rtc_rtsp", "rtsp://homeassistant:8554")

    cameras = []
    for c in config.get("cameras", []):
        rtsp_url = await _resolve_rtsp(c, go2rtc_url, go2rtc_rtsp)
        cameras.append(CameraConfig(
            name=c["name"],
            room=c["room"],
            rtsp_url=rtsp_url,
            frigate_camera=c["frigate_camera"],
            talkback_camera=c.get("talkback_camera", ""),
        ))

    if not cameras:
        log.warning("Aucune caméra configurée — satellite inactif")

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
