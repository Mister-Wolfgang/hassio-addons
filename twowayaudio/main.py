"""Two-Way Audio — generic talkback via go2rtc WebRTC."""
import asyncio
import logging
import os
import tempfile
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from twoway import talk_file, talk_tts, talk_pcm, GO2RTC_API

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger("aioice").setLevel(logging.WARNING)
logging.getLogger("aiortc").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

VOLUME = float(os.environ.get("VOLUME", "1.0"))
TTS_LANGUAGE = os.environ.get("TTS_LANGUAGE", "fr")


@asynccontextmanager
async def lifespan(app: FastAPI):
    import httpx
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            r = await c.get(f"{GO2RTC_API}/api/streams")
        log.info("go2rtc reachable at %s (%d streams)", GO2RTC_API, len(r.json()))
    except Exception as e:
        log.warning("go2rtc NOT reachable at %s: %s", GO2RTC_API, e)
    yield


app = FastAPI(title="Two-Way Audio", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "go2rtc_api": GO2RTC_API}


@app.get("/streams")
async def list_streams():
    """Proxy go2rtc stream list."""
    import httpx
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{GO2RTC_API}/api/streams")
        return r.json()
    except Exception as e:
        raise HTTPException(502, f"go2rtc unreachable: {e}")


@app.post("/talk/{stream}")
async def talk_audio(
    stream: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    """Upload WAV/MP3 and play it on the stream speaker."""
    suffix = "." + (file.filename.rsplit(".", 1)[-1] if file.filename and "." in file.filename else "mp3")
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        f.write(await file.read())
        tmp = f.name

    async def _run():
        try:
            await talk_file(stream, tmp, volume=VOLUME)
        except Exception as e:
            log.error("[%s] talk_file failed: %s", stream, e)
        finally:
            os.unlink(tmp)

    background_tasks.add_task(_run)
    return {"status": "playing", "stream": stream}


class PlayURLRequest(BaseModel):
    url: str


@app.post("/play_url/{stream}")
async def play_url(stream: str, req: PlayURLRequest, background_tasks: BackgroundTasks):
    """Download audio URL and play on stream speaker."""
    async def _run():
        import httpx
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                token = os.environ.get("SUPERVISOR_TOKEN", "")
                headers = {"Authorization": f"Bearer {token}"} if token else {}
                resp = await client.get(req.url, headers=headers)
                resp.raise_for_status()
                suffix = ".mp3" if "mp3" in resp.headers.get("content-type", "") else ".wav"
                with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
                    f.write(resp.content)
                    tmp = f.name
            try:
                await talk_file(stream, tmp, volume=VOLUME)
            finally:
                os.unlink(tmp)
        except Exception as e:
            log.error("[%s] play_url failed: %s", stream, e)

    background_tasks.add_task(_run)
    return {"status": "playing", "stream": stream, "url": req.url}


class TTSRequest(BaseModel):
    text: str
    lang: str = ""


@app.post("/tts/{stream}")
async def tts(stream: str, req: TTSRequest, background_tasks: BackgroundTasks):
    """gTTS text → stream speaker."""
    lang = req.lang or TTS_LANGUAGE

    async def _run():
        try:
            await talk_tts(stream, req.text, lang=lang, volume=VOLUME)
        except Exception as e:
            log.error("[%s] tts failed: %s", stream, e)

    background_tasks.add_task(_run)
    return {"status": "playing", "stream": stream, "text": req.text}
