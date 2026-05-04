"""WebRTC audio session using aiortc — sends audio to camera speaker."""
import asyncio
import fractions
import json
import logging
import math
import os
import re
import struct
import tempfile

import av as _av
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceServer, RTCConfiguration, MediaStreamTrack, RTCRtpSender, VideoStreamTrack
from av import AudioFrame, VideoFrame

log = logging.getLogger(__name__)

SAMPLE_RATE = 8000
CHANNELS = 1
FRAME_SAMPLES = 320   # 40 ms at 8 kHz — matches camera RTP packet size
FRAME_BYTES = FRAME_SAMPLES * CHANNELS * 2   # s16


def _generate_sine_frames(freq_hz: float = 1000.0, duration_s: float = 3.0) -> list[bytes]:
    total_samples = int(SAMPLE_RATE * duration_s)
    chunks = []
    for start in range(0, total_samples, FRAME_SAMPLES):
        frame_pcm = bytearray()
        for i in range(FRAME_SAMPLES):
            val = int(32767 * math.sin(2 * math.pi * freq_hz * (start + i) / SAMPLE_RATE))
            frame_pcm += struct.pack('<h', max(-32768, min(32767, val)))
        chunks.append(bytes(frame_pcm))
    return chunks


class _AudioFileTrack(MediaStreamTrack):
    """Audio track: pre-loaded (from_file/from_tone) or streaming (from_stream).

    Streaming mode: push PCM frames via push_frame(), call finish() when done.
    recv() paces at 40ms wall-clock; in streaming mode waits for frames instead
    of sending silence when the buffer is empty.
    """
    kind = "audio"

    def __init__(self, frames: list[bytes], streaming: bool = False):
        super().__init__()
        self._frames = list(frames)
        self._idx = 0
        self._ts = 0
        self._audio_count = 0
        self._next_send_time: float | None = None
        self._streaming = streaming
        self._stream_queue: asyncio.Queue[bytes | None] = asyncio.Queue() if streaming else None  # type: ignore
        self._done_event = asyncio.Event()
        if not streaming:
            self._done_event.set()  # pre-loaded: done immediately (wait after sleep)

    @classmethod
    def from_stream(cls) -> "_AudioFileTrack":
        """Create a streaming track — feed with push_frame(), close with finish()."""
        return cls([], streaming=True)

    @classmethod
    def from_file(cls, audio_path: str, volume: float = 0.2) -> "_AudioFileTrack":
        return cls(cls._decode(audio_path, volume=volume))

    @classmethod
    def from_tone(cls, freq_hz: float = 1000.0, duration_s: float = 3.0) -> "_AudioFileTrack":
        return cls(_generate_sine_frames(freq_hz, duration_s))

    def push_frame(self, pcm: bytes) -> None:
        """Push a FRAME_BYTES chunk of s16le PCM (streaming mode only)."""
        self._stream_queue.put_nowait(pcm)

    def finish(self) -> None:
        """Signal end of stream."""
        self._stream_queue.put_nowait(None)

    @staticmethod
    def _decode(path: str, volume: float = 0.2) -> list[bytes]:
        import numpy as np
        container = _av.open(path)
        resampler = _av.AudioResampler(format="s16p", layout="mono", rate=SAMPLE_RATE)
        pcm = bytearray()
        for frame in container.decode(audio=0):
            for r in resampler.resample(frame):
                arr = r.to_ndarray().astype('float32') * volume
                pcm.extend(arr[0].clip(-32768, 32767).astype('<i2').tobytes())
        for r in resampler.resample(None):
            arr = r.to_ndarray().astype('float32') * volume
            pcm.extend(arr[0].clip(-32768, 32767).astype('<i2').tobytes())
        container.close()
        chunks = []
        for i in range(0, len(pcm), FRAME_BYTES):
            chunk = bytes(pcm[i:i + FRAME_BYTES])
            if len(chunk) < FRAME_BYTES:
                chunk = chunk + bytes(FRAME_BYTES - len(chunk))
            chunks.append(chunk)
        import struct as _struct
        import numpy as _np
        threshold = int(32767 * 0.01)
        while chunks:
            samples = _struct.unpack(f'<{FRAME_SAMPLES}h', chunks[0])
            if max(abs(s) for s in samples) < threshold:
                chunks.pop(0)
            else:
                break
        fade = 10
        for i, chunk in enumerate(chunks[-fade:]):
            arr = _np.frombuffer(chunk, dtype='<i2').astype('float32')
            arr *= (fade - i) / fade
            chunks[len(chunks) - fade + i] = arr.clip(-32768, 32767).astype('<i2').tobytes()
        return chunks

    @property
    def duration(self) -> float:
        return len(self._frames) * 0.040

    async def recv(self) -> AudioFrame:
        frame = AudioFrame(format="s16", layout="mono", samples=FRAME_SAMPLES)
        frame.pts = self._ts
        frame.sample_rate = SAMPLE_RATE
        frame.time_base = fractions.Fraction(1, SAMPLE_RATE)

        if self._streaming:
            data = await self._stream_queue.get()
            if data is None:
                # stream ended — apply fade-out on last frames via silence padding
                self._done_event.set()
                frame.planes[0].update(bytes(FRAME_BYTES))
            else:
                frame.planes[0].update(data)
                if self._audio_count == 0:
                    log.info("First audio frame (hex): %s", data[:8].hex())
                self._audio_count += 1
        else:
            if self._idx < len(self._frames):
                data = self._frames[self._idx]
                frame.planes[0].update(data)
                if self._audio_count == 0:
                    log.info("First audio frame (hex): %s", data[:8].hex())
                self._idx += 1
                self._audio_count += 1
            else:
                self._done_event.set()
                frame.planes[0].update(bytes(FRAME_BYTES))

        self._ts += FRAME_SAMPLES
        now = asyncio.get_event_loop().time()
        if self._next_send_time is None:
            self._next_send_time = now + 0.040
        else:
            self._next_send_time += 0.040
        delay = self._next_send_time - asyncio.get_event_loop().time()
        if delay > 0:
            await asyncio.sleep(delay)
        return frame


class _BlackVideoTrack(VideoStreamTrack):
    """Minimal black frame video track — added so SDP offer has audio+video like the browser."""

    async def recv(self):
        pts, time_base = await self.next_timestamp()
        frame = VideoFrame(width=320, height=240)
        frame.pts = pts
        frame.time_base = time_base
        return frame


def _patch_rtp_marker() -> None:
    """marker=1 first packet only — patched directly in aiortc/rtcrtpsender.py."""
    log.info("RTP marker patch active (first packet marker=1, rest marker=0)")


def _patch_aioice_interfaces(lan_ip: str) -> None:
    """Force aioice to only use the specified LAN IP for ICE candidates."""
    import aioice.ice as _aioice
    _aioice.get_host_addresses = lambda use_ipv4=True, use_ipv6=True: [lan_ip] if use_ipv4 else []


def _make_pc(ice_servers: list[dict]) -> RTCPeerConnection:
    servers = [
        RTCIceServer(urls=s["urls"], username=s.get("username"), credential=s.get("credential"))
        for s in ice_servers
    ] if ice_servers else []
    return RTCPeerConnection(RTCConfiguration(iceServers=servers))


def _filter_sdp_candidates(sdp: str, allowed_ip: str) -> str:
    """Keep only ICE candidates from allowed_ip — avoids Docker bridge IPs confusing the camera."""
    lines = sdp.splitlines()
    out = []
    for line in lines:
        if line.startswith("a=candidate:"):
            parts = line.split()
            # parts[4] = IP address
            if len(parts) > 4 and parts[4] != allowed_ip:
                continue
        out.append(line)
    eol = "\r\n" if "\r\n" in sdp else "\n"
    return eol.join(out) + eol


def _unify_bundle_ice(sdp: str) -> str:
    """Fix aiortc BUNDLE SDP bug: all m-lines must share the same ice-ufrag/pwd.

    aiortc generates separate ICE credentials per m-line even when BUNDLE is declared.
    The camera expects a single ICE session for the whole BUNDLE group.
    We use the credentials from the first m-line and apply them to all m-lines.
    """
    eol = "\r\n" if "\r\n" in sdp else "\n"
    lines = sdp.replace("\r\n", "\n").splitlines()
    first_ufrag = first_pwd = ""
    for line in lines:
        if line.startswith("a=ice-ufrag:") and not first_ufrag:
            first_ufrag = line
        elif line.startswith("a=ice-pwd:") and not first_pwd:
            first_pwd = line
        if first_ufrag and first_pwd:
            break
    out = []
    for line in lines:
        if line.startswith("a=ice-ufrag:"):
            out.append(first_ufrag)
        elif line.startswith("a=ice-pwd:"):
            out.append(first_pwd)
        else:
            out.append(line)
    return eol.join(out) + eol


def _strip_candidates(sdp: str) -> str:
    """Remove all a=candidate lines from SDP (for trickle ICE offer)."""
    eol = "\r\n" if "\r\n" in sdp else "\n"
    lines = [l for l in sdp.replace("\r\n", "\n").splitlines()
             if not l.startswith("a=candidate:") and not l.startswith("a=end-of-candidates")]
    return eol.join(lines) + eol


def _extract_candidates(sdp: str) -> list[tuple[str, str, int]]:
    """Extract (candidate_str, sdpMid, sdpMLineIndex) from SDP."""
    result = []
    current_mid = "0"
    current_idx = -1
    for line in sdp.replace("\r\n", "\n").splitlines():
        if line.startswith("m="):
            current_idx += 1
        elif line.startswith("a=mid:"):
            current_mid = line.split(":", 1)[1].strip()
        elif line.startswith("a=candidate:"):
            result.append((line[2:], current_mid, current_idx))
    return result


def _inject_video_mline(sdp: str) -> str:
    """Inject a recvonly video m-line sharing the same BUNDLE/ICE transport as audio.

    The camera (KVS SDK) only enters full preview mode (speaker active) when
    both audio and video are negotiated.
    """
    eol = "\r\n" if "\r\n" in sdp else "\n"
    lines = sdp.replace("\r\n", "\n").splitlines()

    ice_ufrag = ice_pwd = fingerprint = ""
    setup = "actpass"
    for line in lines:
        if line.startswith("a=ice-ufrag:") and not ice_ufrag:
            ice_ufrag = line
        elif line.startswith("a=ice-pwd:") and not ice_pwd:
            ice_pwd = line
        elif line.startswith("a=fingerprint:") and not fingerprint:
            fingerprint = line
        elif line.startswith("a=setup:"):
            setup = line.split(":", 1)[1].strip()

    new_lines = []
    for line in lines:
        if line.startswith("a=group:BUNDLE"):
            new_lines.append(line + " 1")
        else:
            new_lines.append(line)

    video_section = [
        "m=video 9 UDP/TLS/RTP/SAVPF 96",
        "c=IN IP4 0.0.0.0",
        "a=rtcp:9 IN IP4 0.0.0.0",
        ice_ufrag,
        ice_pwd,
        "a=ice-options:trickle",
        fingerprint,
        f"a=setup:{setup}",
        "a=mid:1",
        "a=sendrecv",
        "a=rtcp-mux",
        "a=rtcp-rsize",
        "a=rtpmap:96 H264/90000",
        "a=fmtp:96 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
    ]
    new_lines.extend(video_section)
    return eol.join(new_lines) + eol


def _strip_video_from_answer(sdp: str) -> str:
    """Remove the video m-section from the camera's SDP answer."""
    eol = "\r\n" if "\r\n" in sdp else "\n"
    normalized = sdp.replace("\r\n", "\n")

    parts = re.split(r'(?=^m=)', normalized, flags=re.MULTILINE)
    header = parts[0]
    audio_section = next((p for p in parts[1:] if p.startswith("m=audio")), None)

    if audio_section is None:
        log.warning("No audio m-section in SDP answer — using as-is")
        return sdp

    audio_mid = "0"
    for line in audio_section.splitlines():
        if line.startswith("a=mid:"):
            audio_mid = line.split(":", 1)[1].strip()
            break

    header_fixed = re.sub(r'a=group:BUNDLE[^\n]*', f'a=group:BUNDLE {audio_mid}', header)
    result = header_fixed + audio_section
    if eol == "\r\n":
        result = result.replace("\n", "\r\n")
    return result


async def _drain_track(track) -> None:
    """Consume incoming track frames so aiortc doesn't drop the connection."""
    try:
        while True:
            await track.recv()
    except Exception:
        pass


async def _capture_audio_track(track, audio_queue) -> None:
    """Receive audio frames from camera and push raw PCMU bytes to queue."""
    try:
        while True:
            frame = await track.recv()
            for plane in frame.planes:
                audio_queue.put_nowait(bytes(plane))
    except Exception:
        audio_queue.stop()


async def talk_with_track(
    mts,
    track: _AudioFileTrack,
    duration: float,
    audio_queue=None,
) -> None:
    """Run a full MTS/WebRTC session using *track* as the audio source.

    If *audio_queue* is provided, incoming camera audio is forwarded to it
    instead of being drained silently.
    """
    _patch_rtp_marker()
    lan_ip = os.environ.get("LAN_IP", "")
    if lan_ip:
        _patch_aioice_interfaces(lan_ip)
    pc = _make_pc(mts.ice_servers)

    @pc.on("connectionstatechange")
    async def on_state():
        log.info("WebRTC state: %s", pc.connectionState)

    @pc.on("track")
    def on_track(t):
        log.info("Received track from camera: kind=%s", t.kind)
        if t.kind == "audio" and audio_queue is not None:
            asyncio.ensure_future(_capture_audio_track(t, audio_queue))
        else:
            asyncio.ensure_future(_drain_track(t))

    # Audio only transceiver — video m-line injected manually in SDP so aiortc uses single ICE transport
    pc.addTrack(track)

    log.info("Codec: negotiated (audio transceiver + manual video m-line injection)")

    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    gather_done = asyncio.Event()

    @pc.on("icegatheringstatechange")
    def on_gather():
        if pc.iceGatheringState == "complete":
            gather_done.set()

    if pc.iceGatheringState != "complete":
        await asyncio.wait_for(gather_done.wait(), timeout=20)

    local_sdp = pc.localDescription.sdp
    lan_ip = os.environ.get("LAN_IP", "")

    # Extract candidates from gathered SDP to send as trickle after offer
    all_candidates = _extract_candidates(local_sdp)
    trickle_candidates = [(c, mid, idx) for c, mid, idx in all_candidates
                          if not lan_ip or lan_ip in c]

    # Send offer without candidates (trickle ICE like browser)
    sdp_no_cands = _strip_candidates(local_sdp)
    sdp_no_cands = _unify_bundle_ice(sdp_no_cands)
    sdp_no_cands = _inject_video_mline(sdp_no_cands)  # add video m-line so camera negotiates a/v
    sdp_to_send = _filter_sdp_candidates(sdp_no_cands, lan_ip) if lan_ip else sdp_no_cands
    log.debug("SDP offer:\n%s", sdp_to_send)

    sdp_answer_raw = await mts.send_sdp_offer(sdp_to_send)
    import tempfile as _tmp
    _sdp_path = os.path.join(_tmp.gettempdir(), "sdp_answer.txt")
    with open(_sdp_path, "w") as f:
        f.write(sdp_answer_raw)
    log.info("SDP answer written to %s", _sdp_path)

    # Strip video from answer — aiortc only has audio transceiver, single ICE transport
    sdp_answer_audio = _strip_video_from_answer(sdp_answer_raw)
    log.debug("SDP answer (audio only):\n%s", sdp_answer_audio)
    await pc.setRemoteDescription(RTCSessionDescription(sdp=sdp_answer_audio, type="answer"))

    connected = asyncio.Event()
    camera_ready = asyncio.Event()

    @pc.on("connectionstatechange")
    async def on_state2():
        log.info("WebRTC state: %s", pc.connectionState)
        if pc.connectionState in ("connected", "failed", "closed"):
            connected.set()

    async def ws_pump():
        echoed_sids: set = set()
        try:
            while True:
                try:
                    raw = await asyncio.wait_for(mts._ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                data = json.loads(raw)
                log.info("ws_pump msg: %s", json.dumps(data))

                # Handle trickle candidates from camera
                if data.get("method") == "candidate":
                    cand_data = data.get("params", {}).get("candidate", {})
                    if isinstance(cand_data, dict):
                        cand_str = cand_data.get("candidate", "")
                        sdp_mid = cand_data.get("sdpMid", "0")
                        sdp_idx = cand_data.get("sdpMLineIndex", 0)
                    else:
                        cand_str = str(cand_data)
                        sdp_mid = data.get("params", {}).get("sdpMid", "0")
                        sdp_idx = data.get("params", {}).get("sdpMLineIndex", 0)
                    if cand_str:
                        from aiortc import RTCIceCandidate
                        from aiortc.sdp import candidate_from_sdp
                        try:
                            c = candidate_from_sdp(cand_str.split("candidate:", 1)[-1])
                            c.sdpMid = sdp_mid
                            c.sdpMLineIndex = sdp_idx
                            await pc.addIceCandidate(c)
                            log.info("Added remote candidate: %s", cand_str[:80])
                        except Exception as e:
                            log.warning("Failed to add remote candidate: %s", e)
                    continue

                if not camera_ready.is_set() and data.get("errid") == 0 and data.get("errstr") == "Connect Success":
                    log.info("Camera: Connect Success received — sending streams settings")
                    camera_ready.set()
                    # Send settings/preview/streams — required to activate camera stream
                    await mts.send_settings_raw({
                        "sid": mts._settings_sid(),
                        "method": "preview",
                        "streams": [{"channel": 0, "stop": 0, "stream": 0}],
                    })
                    continue

                if data.get("method") == "settings" and data.get("action") == "rsp":
                    sid = data.get("params", {}).get("settings", {}).get("sid")
                    if sid not in echoed_sids:
                        echo = dict(data)
                        echo["action"] = "req"
                        await mts._ws.send(json.dumps(echo))
                        echoed_sids.add(sid)
                        log.debug("Echoed settings rsp sid=%s", sid)
                    else:
                        log.debug("Skipping duplicate settings rsp sid=%s", sid)
        except Exception as e:
            log.debug("ws_pump ended: %s", e)

    # Send localAudio activation via MTS settings after WebRTC connects
    # Browser equivalent: mtsAudioSet({local:[{channel:0, state:1}]}) — no MTS msg sent by browser,
    # but we try a settings/localAudio to see if cloud side requires explicit activation
    async def send_local_audio_activate(channel: int = 0) -> None:
        await asyncio.sleep(0.5)  # wait for stable connection
        try:
            await mts.send_settings_raw({
                "sid": mts._settings_sid(),
                "method": "localAudio",
                "action": "set",
                "params": [{"channel": channel, "state": 1}],
            })
            log.info("Sent localAudio activate for channel %d", channel)
        except Exception as e:
            log.debug("localAudio settings failed (expected if not supported): %s", e)

    # Start ws_pump BEFORE sending our candidates so camera trickle candidates are processed
    pump_task = asyncio.ensure_future(ws_pump())

    # Send our candidates as trickle after answer received
    for cand, mid, idx in trickle_candidates:
        await mts.send_candidate(cand, mid, idx)
        log.info("Trickle candidate mid=%s: %s", mid, cand[:80])

    if pc.connectionState != "connected":
        await asyncio.wait_for(connected.wait(), timeout=30)

    if pc.connectionState != "connected":
        raise RuntimeError(f"WebRTC failed to connect: {pc.connectionState}")

    asyncio.ensure_future(send_local_audio_activate())

    # Wait for track to finish (pre-loaded: duration+2s; streaming: until finish() called)
    if track._streaming:
        await track._done_event.wait()
        await asyncio.sleep(0.5)  # drain last frames
    else:
        await asyncio.sleep(duration + 2.0)
    log.info("Audio done — %d audio frames sent", track._audio_count)
    pump_task.cancel()
    await pc.close()
    await mts.close()
    log.info("Talk session done")


async def talk_file(mts, audio_path: str, volume: float = 0.2) -> None:
    track = _AudioFileTrack.from_file(audio_path, volume=volume)
    await talk_with_track(mts, track, track.duration)


async def talk_tone(mts, freq_hz: float = 1000.0, duration_s: float = 3.0) -> None:
    """Send a sine tone — useful for testing speaker connectivity."""
    log.info("Generating %.0f Hz test tone for %.1fs", freq_hz, duration_s)
    track = _AudioFileTrack.from_tone(freq_hz, duration_s)
    await talk_with_track(mts, track, duration_s)


async def talk_tts(mts, text: str, lang: str = "fr", volume: float = 0.2) -> None:
    from gtts import gTTS
    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
        tmp = f.name
    try:
        gTTS(text=text, lang=lang).save(tmp)
        await talk_file(mts, tmp, volume=volume)
    finally:
        os.unlink(tmp)


async def talk_pcm(mts, pcm_s16le_8k: bytes, volume: float = 1.0) -> None:
    """Play raw s16le 8kHz PCM on camera (e.g. from Wyoming TTS)."""
    import struct as _struct
    # Split into FRAME_BYTES chunks
    chunks = []
    for i in range(0, len(pcm_s16le_8k), FRAME_BYTES):
        chunk = pcm_s16le_8k[i:i + FRAME_BYTES]
        if len(chunk) < FRAME_BYTES:
            chunk = chunk + bytes(FRAME_BYTES - len(chunk))
        if volume != 1.0:
            import numpy as _np
            arr = _np.frombuffer(chunk, dtype='<i2').astype('float32') * volume
            chunk = arr.clip(-32768, 32767).astype('<i2').tobytes()
        chunks.append(chunk)
    # Fade-out
    fade = min(10, len(chunks))
    import numpy as _np
    for i, chunk in enumerate(chunks[-fade:]):
        arr = _np.frombuffer(chunk, dtype='<i2').astype('float32')
        arr *= (fade - i) / fade
        chunks[len(chunks) - fade + i] = arr.clip(-32768, 32767).astype('<i2').tobytes()
    track = _AudioFileTrack(frames=chunks)
    await talk_with_track(mts, track, track.duration)
