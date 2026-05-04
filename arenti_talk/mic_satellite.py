"""Mic satellite — streams camera audio to HA Assist pipeline in a permanent loop."""
import asyncio
import json
import logging
import os
from typing import Callable, Awaitable, AsyncIterator

import numpy as np
import websockets

log = logging.getLogger(__name__)

HA_WS_URL = os.environ.get("HA_WS_URL", "ws://supervisor/core/websocket")
HA_TOKEN  = os.environ.get("SUPERVISOR_TOKEN", "")

TARGET_RATE   = 16000
SOURCE_RATE   = 8000
CHUNK_SAMPLES = 1600    # 100 ms at 16 kHz


# ─── AudioQueue ──────────────────────────────────────────────────────────────

class AudioQueue:
    """Async queue of raw bytes (PCMU 8kHz from RTP or s16le at source_rate)."""

    def __init__(self, maxsize: int = 400):
        self._q: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=maxsize)
        self.source_rate: int = SOURCE_RATE
        self.is_pcmu: bool = True

    def put_nowait(self, data: bytes | None) -> None:
        try:
            self._q.put_nowait(data)
        except asyncio.QueueFull:
            pass

    async def get(self) -> bytes | None:
        return await self._q.get()

    def stop(self) -> None:
        self._q.put_nowait(None)


# ─── audio conversion ─────────────────────────────────────────────────────────

def _pcmu_to_s16(pcmu: bytes) -> bytes:
    samples = np.frombuffer(pcmu, dtype=np.uint8).astype(np.int32)
    samples = ~samples & 0xFF
    sign = (samples & 0x80) >> 7
    exp  = (samples & 0x70) >> 4
    mant = (samples & 0x0F)
    linear = (mant * 2 + 33) << exp
    linear = np.where(sign, -linear, linear)
    return linear.astype(np.int16).tobytes()


def _resample(pcm: bytes, from_rate: int, to_rate: int) -> bytes:
    if from_rate == to_rate:
        return pcm
    arr = np.frombuffer(pcm, dtype=np.int16).astype(np.float32)
    n_out = int(len(arr) * to_rate / from_rate)
    indices = np.linspace(0, len(arr) - 1, n_out)
    return np.interp(indices, np.arange(len(arr)), arr).astype(np.int16).tobytes()


# ─── RTSP source ─────────────────────────────────────────────────────────────

async def pump_rtsp_to_queue(rtsp_url: str, queue: AudioQueue) -> None:
    queue.is_pcmu = False
    queue.source_rate = TARGET_RATE
    cmd = [
        "ffmpeg", "-loglevel", "quiet",
        "-i", rtsp_url, "-vn",
        "-ar", str(TARGET_RATE), "-ac", "1",
        "-f", "s16le", "pipe:1",
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    chunk_bytes = CHUNK_SAMPLES * 2
    try:
        while True:
            data = await proc.stdout.read(chunk_bytes)
            if not data:
                break
            queue.put_nowait(data)
    finally:
        proc.kill()
        queue.stop()


# ─── HA WebSocket auth ────────────────────────────────────────────────────────

async def _ha_auth(ws) -> None:
    await ws.recv()
    await ws.send(json.dumps({"type": "auth", "access_token": HA_TOKEN}))
    resp = json.loads(await ws.recv())
    if resp.get("type") != "auth_ok":
        raise RuntimeError(f"HA auth failed: {resp}")


# ─── single pipeline run ──────────────────────────────────────────────────────

async def _run_one_pipeline(
    ws,
    msg_id: int,
    queue: AudioQueue,
    pipeline_id: str | None,
    on_tts_url: Callable[[str], Awaitable[None]] | None,
    stop_event: asyncio.Event,
) -> None:
    """Run one STT→TTS pipeline pass, streaming from queue until VAD end or stop."""

    cmd: dict = {
        "id": msg_id,
        "type": "assist_pipeline/run",
        "start_stage": "wake_word",
        "end_stage": "tts",
        "input": {
            "sample_rate": TARGET_RATE,
            "no_vad": False,
            "wake_word_timeout": None,
        },
    }
    if pipeline_id:
        cmd["pipeline_id"] = pipeline_id
    await ws.send(json.dumps(cmd))
    log.info("Pipeline %d started (wake_word → tts)", msg_id)

    # Wait for run-start → get stt_binary_handler_id
    handler_id: int | None = None
    while handler_id is None:
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        evt = json.loads(raw)
        log.debug("Pipeline %d recv: %s", msg_id, raw[:200])
        if evt.get("id") != msg_id:
            continue
        if evt.get("type") == "result":
            if not evt.get("success"):
                raise RuntimeError(f"Pipeline rejected: {evt.get('error')}")
            # result ok, continue waiting for events
            continue
        if evt.get("type") == "event":
            e = evt.get("event", {})
            etype = e.get("type")
            log.info("Pipeline %d init-event: %s", msg_id, etype)
            if etype == "run-start":
                d = e.get("data", {})
                handler_id = d.get("stt_binary_handler_id") or d.get("runner_data", {}).get("stt_binary_handler_id")
                log.info("Pipeline %d handler_id=%s data_keys=%s", msg_id, handler_id, list(d.keys()))
            elif etype == "error":
                raise RuntimeError(f"Pipeline start error: {e.get('data')}")

    prefix = bytes([handler_id])
    chunk_bytes = CHUNK_SAMPLES * 2
    buf = bytearray()

    # Stream audio until stop_event or queue empty
    async def _stream():
        nonlocal buf
        while not stop_event.is_set():
            try:
                data = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if data is None:
                break
            pcm = _pcmu_to_s16(data) if queue.is_pcmu else data
            pcm16k = _resample(pcm, queue.source_rate, TARGET_RATE)
            buf.extend(pcm16k)
            while len(buf) >= chunk_bytes:
                chunk = bytes(buf[:chunk_bytes])
                rms = int(np.sqrt(np.mean(np.frombuffer(chunk[1:], dtype=np.int16).astype(np.float32)**2)))
                log.debug("Audio chunk rms=%d bytes=%d", rms, len(chunk))
                await ws.send(prefix + chunk)
                buf = buf[chunk_bytes:]
        await ws.send(prefix)  # end of audio

    stream_task = asyncio.ensure_future(_stream())

    # Collect events
    tts_url: str | None = None
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=20)
            evt = json.loads(raw)
            if evt.get("id") != msg_id:
                continue
            if evt.get("type") != "event":
                continue
            e = evt.get("event", {})
            etype = e.get("type", "")
            log.info("[pipeline %d] event: %s", msg_id, etype)

            if etype == "stt-end":
                stream_task.cancel()

            if etype == "tts-start":
                tts_url = (e.get("data", {}).get("tts_output") or {}).get("url")

            elif etype == "error":
                log.error("[pipeline %d] error: %s", msg_id, e.get("data"))
                break

            elif etype == "run-end":
                break
    finally:
        stream_task.cancel()

    if tts_url and on_tts_url:
        await on_tts_url(tts_url)


# ─── permanent satellite loop ─────────────────────────────────────────────────

async def run_satellite_loop(
    queue: AudioQueue,
    camera_name: str,
    pipeline_id: str | None = None,
    on_tts_url: Callable[[str], Awaitable[None]] | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Permanent loop: reconnects to HA WS and runs pipelines until stop_event set."""
    if stop_event is None:
        stop_event = asyncio.Event()

    msg_id = 1
    retry_delay = 2.0

    while not stop_event.is_set():
        try:
            async with websockets.connect(HA_WS_URL) as ws:
                await _ha_auth(ws)
                log.info("[%s] HA WebSocket connected", camera_name)
                retry_delay = 2.0

                while not stop_event.is_set():
                    try:
                        await _run_one_pipeline(
                            ws, msg_id, queue, pipeline_id, on_tts_url, stop_event
                        )
                        msg_id += 1
                        # Small pause between pipeline runs
                        await asyncio.sleep(0.5)
                    except asyncio.TimeoutError:
                        log.warning("[%s] Pipeline timeout, restarting", camera_name)
                        msg_id += 1
                    except Exception as e:
                        log.error("[%s] Pipeline error: %s", camera_name, e)
                        msg_id += 1
                        await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            break
        except Exception as e:
            if not stop_event.is_set():
                log.error("[%s] WS disconnected: %s — retry in %.0fs", camera_name, e, retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30.0)

    log.info("[%s] Satellite loop stopped", camera_name)


# ─── legacy one-shot (kept for compatibility) ─────────────────────────────────

async def run_satellite(
    queue: AudioQueue,
    pipeline_id: str | None = None,
    on_tts_url: Callable[[str], Awaitable[None]] | None = None,
) -> None:
    stop = asyncio.Event()
    stop.set()  # one-shot: stream until queue empty
    async with websockets.connect(HA_WS_URL) as ws:
        await _ha_auth(ws)
        await _run_one_pipeline(ws, 1, queue, pipeline_id, on_tts_url, stop)
