"""Tapo camera talkback via go2rtc RTSP push and mic via RTSP."""
import asyncio
import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mic_satellite import AudioQueue

log = logging.getLogger(__name__)

GO2RTC_RTSP = os.environ.get("GO2RTC_RTSP", "rtsp://192.168.1.131:8554")


async def tapo_talk_pcm(stream_name: str, pcm_8k: bytes, volume: float = 1.0) -> None:
    """Push PCM s16le 8kHz to go2rtc RTSP → forwarded to Tapo speaker via tapo:// backchannel."""
    import numpy as np

    if volume != 1.0:
        arr = np.frombuffer(pcm_8k, dtype="<i2").astype("float32") * volume
        pcm_8k = arr.clip(-32768, 32767).astype("<i2").tobytes()

    duration = len(pcm_8k) / (8000 * 2) + 3
    url = f"{GO2RTC_RTSP}/{stream_name}"

    cmd = [
        "ffmpeg", "-loglevel", "warning",
        "-f", "s16le", "-ar", "8000", "-ac", "1",
        "-i", "pipe:0",
        "-c:a", "pcm_alaw", "-ar", "8000", "-ac", "1",
        "-f", "rtsp", "-rtsp_transport", "tcp",
        "-sdp_flags", "custom_io",
        url,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, stderr = await asyncio.wait_for(proc.communicate(pcm_8k), timeout=duration)
        if stderr:
            log.debug("[tapo %s] ffmpeg: %s", stream_name, stderr.decode().strip())
    except asyncio.TimeoutError:
        proc.kill()
        log.error("[tapo %s] ffmpeg push timeout", stream_name)


async def tapo_talk_file(stream_name: str, filepath: str, volume: float = 1.0) -> None:
    """Decode audio file → PCM 8kHz and push to go2rtc."""
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
    await tapo_talk_pcm(stream_name, pcm, volume=volume)


async def pump_tapo_mic_to_queue(host: str, password: str, queue: "AudioQueue") -> None:
    """Stream Tapo mic audio via RTSP into AudioQueue as s16le 16kHz."""
    rtsp_url = f"rtsp://wolfgang:{password}@{host}/stream1"
    from mic_satellite import pump_rtsp_to_queue
    await pump_rtsp_to_queue(rtsp_url, queue)
