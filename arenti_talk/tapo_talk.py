"""Tapo camera talkback and audio capture via pytapo."""
import asyncio
import logging
import concurrent.futures
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mic_satellite import AudioQueue

log = logging.getLogger(__name__)

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="tapo")


def _run_in_new_loop(coro):
    """Run an async coroutine in a fresh event loop (for pytapo which calls asyncio.run internally)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def tapo_talk_pcm(host: str, password: str, pcm_8k: bytes, volume: float = 1.0) -> None:
    """Send raw PCM s16le 8kHz to Tapo camera speaker."""
    import numpy as np

    if volume != 1.0:
        arr = np.frombuffer(pcm_8k, dtype="<i2").astype("float32") * volume
        pcm_8k = arr.clip(-32768, 32767).astype("<i2").tobytes()

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_executor, _send_audio_sync, host, password, pcm_8k)


def _send_audio_sync(host: str, password: str, pcm_s16le_8k: bytes) -> None:
    """Blocking in its own thread+loop: authenticate and stream PCM to Tapo."""
    from pytapo import Tapo

    async def _inner():
        tapo = Tapo(host, "admin", password)
        await tapo.startAudioOutput()
        chunk_size = 3200  # 200ms @ 8kHz s16le
        for i in range(0, len(pcm_s16le_8k), chunk_size):
            await tapo.transmitAudio(pcm_s16le_8k[i:i + chunk_size])
        await tapo.stopAudioOutput()

    _run_in_new_loop(_inner())


async def tapo_talk_file(host: str, password: str, filepath: str, volume: float = 1.0) -> None:
    """Decode audio file → PCM 8kHz and send to Tapo speaker."""
    import av
    import numpy as np

    frames = []
    with av.open(filepath) as container:
        resampler = av.AudioResampler(format="s16", layout="mono", rate=8000)
        for frame in container.decode(audio=0):
            for rf in resampler.resample(frame):
                frames.append(rf.to_ndarray().flatten())

    if not frames:
        return
    pcm = np.concatenate(frames).astype("<i2").tobytes()
    await tapo_talk_pcm(host, password, pcm, volume=volume)


async def pump_tapo_mic_to_queue(host: str, password: str, queue: "AudioQueue") -> None:
    """Stream Tapo mic audio (RTSP) into AudioQueue as s16le 16kHz."""
    rtsp_url = f"rtsp://admin:{password}@{host}/stream1"
    from mic_satellite import pump_rtsp_to_queue
    await pump_rtsp_to_queue(rtsp_url, queue)
