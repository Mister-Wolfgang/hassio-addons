"""Generic talkback via go2rtc WebRTC (sendonly audio)."""
import asyncio
import logging
import os

import av
import numpy as np

log = logging.getLogger(__name__)

GO2RTC_API = os.environ.get("GO2RTC_API", "http://192.168.1.131:1984")
SAMPLE_RATE = 8000
FRAME_SAMPLES = 160  # 20ms @ 8kHz
FRAME_BYTES = FRAME_SAMPLES * 2


class _AudioTrack:
    kind = "audio"

    def __init__(self, pcm: bytes):
        self._pcm = pcm
        self._pos = 0
        self._ts = 0

    async def recv(self):
        import fractions
        chunk = self._pcm[self._pos:self._pos + FRAME_BYTES]
        self._pos += FRAME_BYTES
        if not chunk:
            chunk = b'\x00' * FRAME_BYTES

        arr = np.frombuffer(chunk.ljust(FRAME_BYTES, b'\x00'), dtype='<i2')
        frame = av.AudioFrame.from_ndarray(arr.reshape(1, -1), format='s16', layout='mono')
        frame.sample_rate = SAMPLE_RATE
        frame.pts = self._ts
        frame.time_base = fractions.Fraction(1, SAMPLE_RATE)
        self._ts += FRAME_SAMPLES

        await asyncio.sleep(FRAME_SAMPLES / SAMPLE_RATE)
        return frame

    def done(self) -> bool:
        return self._pos >= len(self._pcm)


def _decode_file(path: str, volume: float = 1.0) -> bytes:
    frames = []
    with av.open(path) as container:
        resampler = av.AudioResampler(format='s16', layout='mono', rate=SAMPLE_RATE)
        for frame in container.decode(audio=0):
            for rf in resampler.resample(frame):
                arr = rf.to_ndarray().flatten().astype('float32') * volume
                frames.append(arr.clip(-32768, 32767).astype('<i2'))
    if not frames:
        return b''
    return np.concatenate(frames).tobytes()


async def talk_pcm(stream_name: str, pcm: bytes, volume: float = 1.0) -> None:
    import httpx
    from aiortc import RTCPeerConnection, RTCSessionDescription

    if volume != 1.0:
        arr = np.frombuffer(pcm, dtype='<i2').astype('float32') * volume
        pcm = arr.clip(-32768, 32767).astype('<i2').tobytes()

    duration = len(pcm) / (SAMPLE_RATE * 2)
    track = _AudioTrack(pcm)

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

    await pc.setRemoteDescription(RTCSessionDescription(sdp=resp.text, type="answer"))
    log.info("[%s] playing %.1fs", stream_name, duration)
    await asyncio.sleep(duration + 0.5)
    await pc.close()


async def talk_file(stream_name: str, path: str, volume: float = 1.0) -> None:
    pcm = _decode_file(path, volume=volume)
    if pcm:
        await talk_pcm(stream_name, pcm, volume=1.0)


async def talk_tts(stream_name: str, text: str, lang: str = "fr", volume: float = 1.0) -> None:
    import tempfile, os
    from gtts import gTTS
    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
        tmp = f.name
    try:
        gTTS(text=text, lang=lang).save(tmp)
        await talk_file(stream_name, tmp, volume=volume)
    finally:
        os.unlink(tmp)
