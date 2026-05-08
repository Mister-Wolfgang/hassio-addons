"""Entry point FastAPI — health check + démarrage du satellite."""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from satellite import MultiMicSatellite, CameraConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

OPTIONS_PATH = os.environ.get("OPTIONS_PATH", "/data/options.json")
_satellite: MultiMicSatellite | None = None


def _load_config() -> dict:
    with open(OPTIONS_PATH) as f:
        return json.load(f)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _satellite

    config = _load_config()
    cameras = [
        CameraConfig(
            name=c["name"],
            room=c["room"],
            rtsp_url=c["rtsp_url"],
            frigate_camera=c["frigate_camera"],
            talkback_camera=c.get("talkback_camera", ""),
        )
        for c in config.get("cameras", [])
    ]

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
