"""Arenti Talk — FastAPI service: speaker + mic satellite for Arenti/Meari cameras."""
import asyncio
import json
import logging
import os
import shutil
import tempfile
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from auth import ArentiSession
from mts import MTSSession
from webrtc_talk import talk_file, talk_tts, talk_with_track, talk_pcm, _AudioFileTrack, FRAME_BYTES
from mic_satellite import AudioQueue, run_satellite_loop, pump_rtsp_to_queue
from wyoming_tts import synthesize_to_pcm

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("aioice").setLevel(logging.WARNING)
logging.getLogger("aiortc").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)

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
    LISTEN_ENABLED  = bool(_opts.get("listen_enabled", True))
else:
    USERNAME        = os.environ["ARENTI_USER"]
    PASSWORD        = os.environ["ARENTI_PASS"]
    VOLUME          = float(os.environ.get("ARENTI_VOLUME", "0.2"))
    WYOMING_TTS_URI = os.environ.get("WYOMING_TTS_URI", "")
    TTS_VOICE       = os.environ.get("TTS_VOICE", "")
    TTS_LANGUAGE    = os.environ.get("TTS_LANGUAGE", "fr")
    LISTEN_ENABLED  = os.environ.get("LISTEN_ENABLED", "true").lower() == "true"

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
        if dev.get("asFriend", False):
            log.info("Camera '%s' is shared (asFriend) — skipped", name)
            continue
        ov = overrides.get(name, {})
        CAMERAS[name] = {
            "device_id":      str(dev.get("deviceID", dev.get("deviceId", dev.get("deviceid", "")))),
            "host_key":       dev.get("hostKey", ""),
            "sn_num":         sn,
            "audio_source":   ov.get("audio_source", "arenti"),
            "pipeline_id":    ov.get("pipeline_id", None),
            "listen_enabled": bool(ov.get("listen_enabled", True)),
        }
    log.info("Discovered %d camera(s): %s", len(CAMERAS), list(CAMERAS))


def _install_custom_component() -> None:
    src = "/app/custom_components/arenti_talk"
    dst = "/config/custom_components/arenti_talk"
    if not os.path.isdir(src):
        return
    try:
        if os.path.isdir(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
        log.info("Custom component installed to %s", dst)
    except Exception as e:
        log.error("Failed to install custom component: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _install_custom_component()
    await _discover_cameras()
    yield
    for task in _listen_tasks.values():
        task.cancel()
    if _session:
        await _session.close()


app = FastAPI(title="2way Audio Arenti", lifespan=lifespan)


@app.middleware("http")
async def strip_ingress_prefix(request, call_next):
    path = request.scope["path"]
    # Normalize double slashes and empty paths to "/"
    import re
    path = re.sub(r"//+", "/", path) or "/"
    request.scope["path"] = path
    return await call_next(request)


@app.get("/")
async def index():
    return FileResponse("/app/static/index.html")

app.mount("/static", StaticFiles(directory="/app/static"), name="static")


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
    """Permanent mic satellite: WebRTC → HA Assist pipeline loop."""
    if not LISTEN_ENABLED:
        log.info("[%s] Listen disabled globally", camera_name)
        return
    if not cam.get("listen_enabled", True):
        log.info("[%s] Listen disabled for this camera", camera_name)
        return

    pipeline_id  = cam.get("pipeline_id")
    audio_source = cam.get("audio_source", "arenti")
    stop_event   = asyncio.Event()

    async def on_tts_url(url: str) -> None:
        log.info("[%s] TTS response: %s", camera_name, url)
        try:
            await _play_url_on_camera(cam, url)
        except Exception as e:
            log.error("[%s] Failed to play TTS: %s", camera_name, e)

    try:
        if audio_source == "arenti":
            while not stop_event.is_set():
                queue = AudioQueue()
                silence_track = _AudioFileTrack(frames=[])
                try:
                    mts = await _mts_for(cam)
                except Exception as e:
                    log.error("[%s] MTS connect failed: %s — retry in 10s", camera_name, e)
                    await asyncio.sleep(10)
                    continue
                inner_stop = asyncio.Event()
                webrtc_task = asyncio.ensure_future(
                    talk_with_track(mts, silence_track, duration=3600.0, audio_queue=queue)
                )
                sat_task = asyncio.ensure_future(
                    run_satellite_loop(queue, camera_name,
                                       pipeline_id=pipeline_id,
                                       on_tts_url=on_tts_url,
                                       stop_event=inner_stop)
                )

                def _on_webrtc_done(t):
                    # WebRTC ended (failed/closed) → stop satellite too
                    inner_stop.set()
                    queue.stop()

                webrtc_task.add_done_callback(_on_webrtc_done)

                try:
                    await asyncio.gather(webrtc_task, sat_task)
                except asyncio.CancelledError:
                    inner_stop.set()
                    queue.stop()
                    break
                except Exception as e:
                    log.error("[%s] Session error: %s — restarting in 5s", camera_name, e)
                finally:
                    webrtc_task.cancel()
                    sat_task.cancel()
                    inner_stop.set()
                    queue.stop()

                if stop_event.is_set():
                    break
                log.info("[%s] WebRTC session ended — restarting in 5s", camera_name)
                await asyncio.sleep(5)
        else:
            queue = AudioQueue()
            rtsp_task = asyncio.ensure_future(pump_rtsp_to_queue(audio_source, queue))
            try:
                await run_satellite_loop(queue, camera_name,
                                          pipeline_id=pipeline_id,
                                          on_tts_url=on_tts_url,
                                          stop_event=stop_event)
            finally:
                rtsp_task.cancel()
    finally:
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
        except TimeoutError:
            log.error("[%s] WebRTC connection timeout", camera)
        except Exception as e:
            log.error("[%s] talk_file failed: %s", camera, e)
        finally:
            os.unlink(tmp)

    background_tasks.add_task(_run)
    return {"status": "playing", "camera": camera}


class PlayURLRequest(BaseModel):
    url: str


@app.post("/play_url/{camera}")
async def play_url(camera: str, req: PlayURLRequest, background_tasks: BackgroundTasks):
    """Download audio URL and play on camera speaker."""
    cam = _get_camera(camera)

    async def _run():
        import numpy as _np
        log.info("[%s] Streaming URL: %s", camera, req.url[:80])
        track = _AudioFileTrack.from_stream()

        async def _ffmpeg_decode():
            """Stream URL → ffmpeg → s16le 8kHz → push frames to track."""
            token = os.environ.get("SUPERVISOR_TOKEN", "")
            cmd = [
                "ffmpeg", "-loglevel", "warning",
                "-headers", f"Authorization: Bearer {token}\r\n",
                "-i", req.url,
                "-vn", "-ar", "8000", "-ac", "1", "-f", "s16le", "pipe:1",
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            async def _log_stderr():
                async for line in proc.stderr:
                    log.warning("[%s] ffmpeg: %s", camera, line.decode().rstrip())
            asyncio.ensure_future(_log_stderr())
            buf = bytearray()
            try:
                while True:
                    chunk = await proc.stdout.read(FRAME_BYTES * 4)
                    if not chunk:
                        break
                    buf.extend(chunk)
                    while len(buf) >= FRAME_BYTES:
                        raw = bytes(buf[:FRAME_BYTES])
                        buf = buf[FRAME_BYTES:]
                        if VOLUME != 1.0:
                            arr = _np.frombuffer(raw, dtype='<i2').astype('float32') * VOLUME
                            raw = arr.clip(-32768, 32767).astype('<i2').tobytes()
                        track.push_frame(raw)
            except Exception as e:
                log.error("[%s] ffmpeg decode error: %s", camera, e)
            finally:
                proc.kill()
                track.finish()
                log.info("[%s] ffmpeg stream ended", camera)

        try:
            mts = await _mts_for(cam)
            decode_task = asyncio.ensure_future(_ffmpeg_decode())
            try:
                await talk_with_track(mts, track, duration=0, audio_queue=None)
            finally:
                decode_task.cancel()
        except TimeoutError:
            log.error("[%s] WebRTC connection timeout", camera)
        except Exception as e:
            log.error("[%s] play_url failed: %s", camera, e)

    background_tasks.add_task(_run)
    return {"status": "playing", "camera": camera, "url": req.url}


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
        try:
            mts = await _mts_for(cam)
            uri = req.wyoming_uri or WYOMING_TTS_URI
            if uri:
                voice = req.voice or TTS_VOICE or None
                lang  = req.lang or TTS_LANGUAGE
                pcm = await synthesize_to_pcm(uri, req.text, voice=voice, language=lang)
                await talk_pcm(mts, pcm, volume=VOLUME)
            else:
                await talk_tts(mts, req.text, req.lang, volume=VOLUME)
        except TimeoutError:
            log.error("[%s] WebRTC connection timeout (camera unreachable?)", camera)
        except Exception as e:
            log.error("[%s] TTS failed: %s", camera, e)

    background_tasks.add_task(_run)
    return {"status": "playing", "camera": camera, "text": req.text}


@app.post("/listen/{camera}")
async def start_listen(camera: str):
    """Start permanent mic satellite session."""
    cam = _get_camera(camera)
    if not LISTEN_ENABLED:
        raise HTTPException(403, "Listen disabled globally")
    if not cam.get("listen_enabled", True):
        raise HTTPException(403, f"Listen disabled for camera '{camera}'")
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
