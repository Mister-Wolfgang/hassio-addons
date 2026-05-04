"""Mic satellite — streams camera audio to HA Assist pipeline, returns TTS URL."""
import asyncio
import json
import logging
import os
import subprocess
from typing import AsyncIterator, Callable, Awaitable

import numpy as np
import websockets

log = logging.getLogger(__name__)

HA_WS_URL = os.environ.get("HA_WS_URL", "ws://supervisor/core/websocket")
HA_TOKEN  = os.environ.get("SUPERVISOR_TOKEN", "")

TARGET_RATE   = 16000
SOURCE_RATE   = 8000
CHUNK_SAMPLES = 1600    # 100 ms at 16 kHz


class AudioQueue:
    """Async queue of raw bytes (PCMU 8kHz from camera RTP or s16le 16kHz from RTSP)."""

    def __init__(self, maxsize: int = 400):
        self._q: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=maxsize)
        self.source_rate: int = SOURCE_RATE
        self.is_pcmu: bool = True   # False = already s16le at source_rate

    def put_nowait(self, data: bytes | None) -> None:
        try:
            self._q.put_nowait(data)
        except asyncio.QueueFull:
            pass

    async def get(self) -> bytes | None:
        return await self._q.get()

    def stop(self) -> None:
        self._q.put_nowait(None)


# ─── audio conversion ────────────────────────────────────────────────────────

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
    ratio = to_rate / from_rate
    n_out = int(len(arr) * ratio)
    indices = np.linspace(0, len(arr) - 1, n_out)
    resampled = np.interp(indices, np.arange(len(arr)), arr)
    return resampled.astype(np.int16).tobytes()


async def _audio_chunks(queue: AudioQueue) -> AsyncIterator[bytes]:
    chunk_bytes = CHUNK_SAMPLES * 2
    buf = bytearray()
    while True:
        data = await queue.get()
        if data is None:
            break
        pcm = _pcmu_to_s16(data) if queue.is_pcmu else data
        pcm16k = _resample(pcm, queue.source_rate, TARGET_RATE)
        buf.extend(pcm16k)
        while len(buf) >= chunk_bytes:
            yield bytes(buf[:chunk_bytes])
            buf = buf[chunk_bytes:]


# ─── RTSP audio source ───────────────────────────────────────────────────────

async def pump_rtsp_to_queue(rtsp_url: str, queue: AudioQueue) -> None:
    """Read RTSP audio via ffmpeg subprocess → push s16le 16kHz chunks to queue."""
    queue.is_pcmu = False
    queue.source_rate = TARGET_RATE
    cmd = [
        "ffmpeg", "-loglevel", "quiet",
        "-i", rtsp_url,
        "-vn",
        "-ar", str(TARGET_RATE),
        "-ac", "1",
        "-f", "s16le",
        "pipe:1",
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


# ─── HA Assist pipeline ──────────────────────────────────────────────────────

async def run_satellite(
    queue: AudioQueue,
    pipeline_id: str | None = None,
    on_tts_url: Callable[[str], Awaitable[None]] | None = None,
) -> None:
    """Connect to HA WebSocket, stream audio, call on_tts_url with TTS audio URL."""
    async with websockets.connect(HA_WS_URL) as ws:
        # Auth
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": HA_TOKEN}))
        resp = await ws.recv()
        if '"auth_ok"' not in resp:
            raise RuntimeError(f"HA auth failed: {resp}")
        log.info("HA WebSocket authenticated")

        # Start pipeline
        cmd: dict = {
            "id": 1,
            "type": "assist_pipeline/run",
            "start_stage": "stt",
            "end_stage": "tts",
            "input": {"sample_rate": TARGET_RATE},
        }
        if pipeline_id:
            cmd["pipeline_id"] = pipeline_id
        await ws.send(json.dumps(cmd))

        # Get stt_binary_handler_id from run-start event
        handler_id: int | None = None
        while handler_id is None:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            evt = json.loads(raw)
            if evt.get("type") == "event":
                e = evt.get("event", {})
                if e.get("type") == "run-start":
                    handler_id = e["data"]["stt_binary_handler_id"]
                    log.info("Pipeline started handler_id=%d", handler_id)

        prefix = bytes([handler_id])

        # Stream audio to pipeline
        async for chunk in _audio_chunks(queue):
            await ws.send(prefix + chunk)
        await ws.send(prefix)  # end of audio signal
        log.info("Audio stream ended")

        # Collect pipeline events until run-end
        tts_url: str | None = None
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
            except asyncio.TimeoutError:
                log.warning("Pipeline response timeout")
                break
            evt = json.loads(raw)
            if evt.get("type") != "event":
                continue
            e = evt.get("event", {})
            etype = e.get("type", "")
            log.info("Pipeline event: %s", etype)

            if etype == "tts-start":
                tts_url = e.get("data", {}).get("tts_output", {}).get("url")
                log.info("TTS URL: %s", tts_url)
            elif etype == "error":
                log.error("Pipeline error: %s", e.get("data"))
                break
            elif etype == "run-end":
                break

        if tts_url and on_tts_url:
            await on_tts_url(tts_url)
