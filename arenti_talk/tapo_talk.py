"""Tapo camera talkback via go2rtc WebRTC (sendonly audio) and mic via RTSP."""
import asyncio
import logging
import os
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mic_satellite import AudioQueue

log = logging.getLogger(__name__)

GO2RTC_API  = os.environ.get("GO2RTC_API",  "http://192.168.1.131:1984")
GO2RTC_RTSP = os.environ.get("GO2RTC_RTSP", "rtsp://192.168.1.131:8554")


class _PCMATrack:
    """aiortc-compatible audio track that plays raw PCM s16le 8kHz."""
    kind = "audio"

    def __init__(self, pcm_s16le: bytes):
        from aiortc.mediastreams import AudioStreamTrack
        import fractions
        self._pcm = pcm_s16le
        self._pos = 0
        self._ts  = 0
        self._clock_rate = 8000
        self._frame_samples = 160  # 20ms @ 8kHz

    async def recv(self):
        import av, fractions
        frame_bytes = self._frame_samples * 2  # s16le = 2 bytes/sample
        chunk = self._pcm[self._pos:self._pos + frame_bytes]
        self._pos += frame_bytes

        if not chunk:
            # silence to signal end
            chunk = b'\x00' * frame_bytes

        import numpy as np
        arr = np.frombuffer(chunk.ljust(frame_bytes, b'\x00'), dtype='<i2')
        frame = av.AudioFrame.from_ndarray(arr.reshape(1, -1), format='s16', layout='mono')
        frame.sample_rate = self._clock_rate
        frame.pts = self._ts
        frame.time_base = fractions.Fraction(1, self._clock_rate)
        self._ts += self._frame_samples

        # pace at real time
        await asyncio.sleep(self._frame_samples / self._clock_rate)
        return frame

    def done(self) -> bool:
        return self._pos >= len(self._pcm)


async def tapo_talk_pcm(stream_name: str, pcm_8k: bytes, volume: float = 1.0) -> None:
    """Send PCM s16le 8kHz to Tapo speaker via go2rtc WebRTC (sendonly)."""
    import traceback
    try:
        import httpx
        from aiortc import RTCPeerConnection, RTCSessionDescription
        import numpy as np
        log.info("[tapo %s] imports ok, GO2RTC_API=%s", stream_name, GO2RTC_API)
    except Exception:
        log.error("[tapo %s] import error:\n%s", stream_name, traceback.format_exc())
        return

    if volume != 1.0:
        arr = np.frombuffer(pcm_8k, dtype='<i2').astype('float32') * volume
        pcm_8k = arr.clip(-32768, 32767).astype('<i2').tobytes()

    duration = len(pcm_8k) / (8000 * 2)
    track = _PCMATrack(pcm_8k)

    pc = RTCPeerConnection()
    pc.addTrack(track)

    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{GO2RTC_API}/api/webrtc?src={stream_name}",
            content=pc.localDescription.sdp,
            headers={"Content-Type": "application/sdp"},
        )
        resp.raise_for_status()
        answer_sdp = resp.text

    await pc.setRemoteDescription(RTCSessionDescription(sdp=answer_sdp, type="answer"))

    log.info("[tapo %s] WebRTC connected, playing %.1fs audio", stream_name, duration)
    await asyncio.sleep(duration + 0.5)
    await pc.close()
    log.info("[tapo %s] WebRTC closed", stream_name)


async def tapo_talk_file(stream_name: str, filepath: str, volume: float = 1.0) -> None:
    """Decode audio file → PCM 8kHz and send to Tapo via go2rtc WebRTC."""
    import av
    import numpy as np

    frames = []
    with av.open(filepath) as container:
        resampler = av.AudioResampler(format='s16', layout='mono', rate=8000)
        for frame in container.decode(audio=0):
            for rf in resampler.resample(frame):
                frames.append(rf.to_ndarray().flatten())

    if not frames:
        return
    pcm = np.concatenate(frames).astype('<i2').tobytes()
    await tapo_talk_pcm(stream_name, pcm, volume=volume)


async def pump_tapo_mic_to_queue(host: str, password: str, queue: "AudioQueue") -> None:
    """Stream Tapo mic audio via RTSP into AudioQueue as s16le 16kHz."""
    rtsp_url = f"rtsp://wolfgang:{password}@{host}/stream1"
    from mic_satellite import pump_rtsp_to_queue
    await pump_rtsp_to_queue(rtsp_url, queue)
