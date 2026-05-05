"""Tapo camera talkback and audio capture via pytapo."""
import asyncio
import logging
import audioop
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mic_satellite import AudioQueue

log = logging.getLogger(__name__)


async def tapo_talk_pcm(host: str, password: str, pcm_8k: bytes, volume: float = 1.0) -> None:
    """Send raw PCM s16le 8kHz to Tapo camera speaker."""
    from pytapo import Tapo
    import numpy as np

    if volume != 1.0:
        arr = np.frombuffer(pcm_8k, dtype="<i2").astype("float32") * volume
        pcm_8k = arr.clip(-32768, 32767).astype("<i2").tobytes()

    tapo = Tapo(host, "admin", password)
    await asyncio.get_event_loop().run_in_executor(None, _send_audio_sync, tapo, pcm_8k)


def _send_audio_sync(tapo, pcm_s16le_8k: bytes) -> None:
    """Blocking: encode PCM → µ-law and stream to Tapo."""
    # Convert s16le → µ-law (PCMU) for Tapo
    pcmu = audioop.ulaw2lin(audioop.lin2ulaw(pcm_s16le_8k, 2), 2)
    # pytapo expects raw PCM s16le — revert to s16le after µ-law round-trip
    tapo.startAudioOutput()
    chunk_size = 3200  # 200ms @ 8kHz s16le
    for i in range(0, len(pcm_s16le_8k), chunk_size):
        tapo.transmitAudio(pcm_s16le_8k[i:i + chunk_size])
    tapo.stopAudioOutput()


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
