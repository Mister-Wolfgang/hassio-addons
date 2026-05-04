"""Arenti Talk — FastAPI service: speaker + mic satellite for Arenti/Meari cameras."""
import asyncio
import json
import logging
import os
import tempfile
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks
from pydantic import BaseModel

from auth import ArentiSession
from mts import MTSSession
from webrtc_talk import talk_file, talk_tts, talk_with_track, talk_pcm, _AudioFileTrack
from mic_satellite import AudioQueue, run_satellite, pump_rtsp_to_queue
from wyoming_tts import synthesize_to_pcm

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ─── config ────────────────────────────────────────────────────────────────

_OPTIONS_PATH = os.environ.get("OPTIONS_PATH", "/data/options.json")

if os.path.exists(_OPTIONS_PATH):
    with open(_OPTIONS_PATH) as _f:
        _opts = json.load(_f)
    USERNAME        = _opts["arenti_email"]
    PASSWORD        = _opts["arenti_password"]
    VOLUME          = float(_opts.get("volume", 0.2))
    WYOMING_TTS_URI = _opts.get("wyoming_tts_uri", "")
    TTS_VOICE       = _opts.get("tts_voice", "")
    TTS_LANGUAGE    = _opts.get("tts_language", "fr")
else:
    USERNAME        = os.environ["ARENTI_USER"]
    PASSWORD        = os.environ["ARENTI_PASS"]
    VOLUME          = float(os.environ.get("ARENTI_VOLUME", "0.2"))
    WYOMING_TTS_URI = os.environ.get("WYOMING_TTS_URI", "")
    TTS_VOICE       = os.environ.get("TTS_VOICE", "")
    TTS_LANGUAGE    = os.environ.get("TTS_LANGUAGE", "fr")

CAMERAS: dict[str, dict] = {}

# Active listen sessions: camera_name → asyncio.Task
_listen_tasks: dict[str, asyncio.Task] = {}

_session: ArentiSession | None = None


# ─── session / discovery ─────────────────────────────────────────────────────

async def get_session() -> ArentiSession:
    global _session
    if _session is None or not _session.user_token:
        _session = ArentiSession(USERNAME, PASSWORD)
        await _session.login()
        log.info("Logged in as %s (userId=%s)", USERNAME, _session.user_id)
    return _session


async def _discover_cameras() -> None:
    sess = await get_session()
    data = await sess.get("/device/list", params={"userId": sess.user_id})
    devices = (data.get("data") or {}).get("ipc", [])

    overrides: dict[str, dict] = {}
    if os.path.exists(_OPTIONS_PATH):
        with open(_OPTIONS_PATH) as _f:
            for ov in json.load(_f).get("camera_overrides", []):
                overrides[ov["name"].lower().replace(" ", "_")] = ov

    import unicodedata
    seen_sn: set[str] = set()
    for dev in devices:
        sn = dev.get("snNum", "")
        if sn in seen_sn:
            continue
        seen_sn.add(sn)
        raw = dev.get("deviceName", sn or "unknown")
        name = unicodedata.normalize("NFD", raw).encode("ascii", "ignore").decode().lower().replace(" ", "_")
        # ensure unique name if two devices share the same normalized name
        if name in CAMERAS:
            name = f"{name}_{sn[-4:]}"
        ov = overrides.get(name, {})
        CAMERAS[name] = {
            "device_id":    str(dev.get("deviceID", dev.get("deviceId", dev.get("deviceid", "")))),
            "host_key":     dev.get("hostKey", ""),
            "sn_num":       sn,
            "audio_source": ov.get("audio_source", "arenti"),
            "pipeline_id":  ov.get("pipeline_id", None),
        }
    log.info("Discovered %d camera(s): %s", len(CAMERAS), list(CAMERAS))


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _discover_cameras()
    yield
    for task in _listen_tasks.values():
        task.cancel()
    if _session:
        await _session.close()


app = FastAPI(title="2way Audio Arenti", lifespan=lifespan)


# ─── helpers ────────────────────────────────────────────────────────────────

def _get_camera(name: str) -> dict:
    cam = CAMERAS.get(name)
    if not cam or not cam["device_id"]:
        raise HTTPException(404, f"Camera '{name}' not configured")
    return cam


async def _mts_for(cam: dict) -> MTSSession:
    sess = await get_session()
    mts = MTSSession(sess=sess, device_id=cam["device_id"], device_code=cam["sn_num"])
    await mts.wake_up()
    await mts.get_host_key()
    await mts.get_wss_token()
    await mts.connect()
    await mts.handshake()
    return mts


async def _play_url_on_camera(cam: dict, url: str) -> None:
    """Download a URL and play it on the camera speaker."""
    import httpx, os
    # url may be relative (/api/tts_proxy/...) — prepend supervisor base
    if url.startswith("/"):
        base = os.environ.get("HA_BASE_URL", "http://supervisor/core")
        url = base + url
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            url,
            headers={"Authorization": f"Bearer {os.environ.get('SUPERVISOR_TOKEN', '')}"},
        )
        resp.raise_for_status()
        suffix = ".mp3" if "mp3" in resp.headers.get("content-type", "") else ".wav"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            f.write(resp.content)
            tmp = f.name
    try:
        mts = await _mts_for(cam)
        await talk_file(mts, tmp)
    finally:
        os.unlink(tmp)


# ─── listen session ──────────────────────────────────────────────────────────

async def _listen_session(cam: dict, camera_name: str) -> None:
    """Open a WebRTC session (or RTSP), stream mic to HA, play TTS response."""
    queue = AudioQueue()
    pipeline_id = cam.get("pipeline_id")
    audio_source = cam.get("audio_source", "arenti")

    async def on_tts_url(url: str) -> None:
        log.info("[%s] TTS response: %s", camera_name, url)
        try:
            await _play_url_on_camera(cam, url)
        except Exception as e:
            log.error("[%s] Failed to play TTS: %s", camera_name, e)

    if audio_source == "arenti":
        # Open WebRTC session — silence track keeps connection alive while mic is captured
        silence_track = _AudioFileTrack(frames=[])
        mts = await _mts_for(cam)
        webrtc_task = asyncio.ensure_future(
            talk_with_track(mts, silence_track, duration=3600.0, audio_queue=queue)
        )
        try:
            await run_satellite(queue, pipeline_id=pipeline_id, on_tts_url=on_tts_url)
        finally:
            webrtc_task.cancel()
    else:
        # RTSP or custom source
        rtsp_url = audio_source if audio_source.startswith("rtsp") else audio_source
        rtsp_task = asyncio.ensure_future(pump_rtsp_to_queue(rtsp_url, queue))
        try:
            await run_satellite(queue, pipeline_id=pipeline_id, on_tts_url=on_tts_url)
        finally:
            rtsp_task.cancel()

    _listen_tasks.pop(camera_name, None)
    log.info("[%s] Listen session ended", camera_name)


# ─── endpoints ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    sess = await get_session()
    return {"status": "ok", "userId": sess.user_id}


@app.get("/cameras")
async def list_cameras():
    return CAMERAS


@app.post("/cameras/refresh")
async def refresh_cameras():
    CAMERAS.clear()
    await _discover_cameras()
    return CAMERAS


@app.post("/talk/{camera}")
async def talk_audio(
    camera: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    """Upload WAV/MP3 and play it on the camera speaker."""
    cam = _get_camera(camera)
    suffix = "." + (file.filename.rsplit(".", 1)[-1] if "." in file.filename else "mp3")
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        f.write(await file.read())
        tmp = f.name

    async def _run():
        try:
            mts = await _mts_for(cam)
            await talk_file(mts, tmp, volume=VOLUME)
        finally:
            os.unlink(tmp)

    background_tasks.add_task(_run)
    return {"status": "playing", "camera": camera}


class TTSRequest(BaseModel):
    text: str
    lang: str = "fr"
    voice: str = ""         # Wyoming voice name override
    wyoming_uri: str = ""   # Wyoming URI override


@app.post("/tts/{camera}")
async def talk_text(camera: str, req: TTSRequest, background_tasks: BackgroundTasks):
    """TTS text → camera speaker. Uses Wyoming TTS if configured, else gTTS."""
    cam = _get_camera(camera)

    async def _run():
        mts = await _mts_for(cam)
        uri = req.wyoming_uri or WYOMING_TTS_URI
        if uri:
            voice = req.voice or TTS_VOICE or None
            lang  = req.lang or TTS_LANGUAGE
            pcm = await synthesize_to_pcm(uri, req.text, voice=voice, language=lang)
            await talk_pcm(mts, pcm, volume=VOLUME)
        else:
            await talk_tts(mts, req.text, req.lang, volume=VOLUME)

    background_tasks.add_task(_run)
    return {"status": "playing", "camera": camera, "text": req.text}


@app.post("/listen/{camera}")
async def start_listen(camera: str):
    """Start mic satellite session: camera mic → HA Assist pipeline → speaker."""
    cam = _get_camera(camera)
    if camera in _listen_tasks and not _listen_tasks[camera].done():
        return {"status": "already_listening", "camera": camera}
    task = asyncio.ensure_future(_listen_session(cam, camera))
    _listen_tasks[camera] = task
    return {"status": "listening", "camera": camera}


@app.delete("/listen/{camera}")
async def stop_listen(camera: str):
    """Stop active mic satellite session."""
    task = _listen_tasks.pop(camera, None)
    if task and not task.done():
        task.cancel()
        return {"status": "stopped", "camera": camera}
    return {"status": "not_listening", "camera": camera}


@app.get("/listen")
async def list_listen():
    """List active listen sessions."""
    return {
        cam: "listening" if not t.done() else "done"
        for cam, t in _listen_tasks.items()
    }


@app.get("/devices")
async def list_devices():
    sess = await get_session()
    return await sess.get("/device/list", params={"userId": sess.user_id})
