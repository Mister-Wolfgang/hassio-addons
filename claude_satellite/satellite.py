"""Multi-mic satellite: surveille tous les streams caméras pour le wake word."""
import asyncio
import logging
import time
from dataclasses import dataclass, field

import numpy as np

from wyoming_proto import WyomingClient

log = logging.getLogger(__name__)

RATE = 16000
WIDTH = 2
CHANNELS = 1
CHUNK_MS = 100
CHUNK_BYTES = int(RATE * WIDTH * CHANNELS * CHUNK_MS / 1000)
RMS_WINDOW_CHUNKS = int(2000 / CHUNK_MS)   # 2s glissantes
PIPELINE_DEBOUNCE_S = 3.0


@dataclass
class CameraConfig:
    name: str
    room: str
    rtsp_url: str
    frigate_camera: str
    talkback_camera: str = ""


@dataclass
class WakeEvent:
    camera: CameraConfig
    ww_score: float
    rms: float
    timestamp: float
    all_scores: dict = field(default_factory=dict)
    all_rms: dict = field(default_factory=dict)


class RMSBuffer:
    """Fenêtre glissante pour calculer le niveau RMS."""

    def __init__(self, n_chunks: int = RMS_WINDOW_CHUNKS):
        self._chunks: list[bytes] = []
        self._n = n_chunks

    def push(self, data: bytes):
        self._chunks.append(data)
        if len(self._chunks) > self._n:
            self._chunks.pop(0)

    def rms(self) -> float:
        if not self._chunks:
            return 0.0
        arr = np.frombuffer(b"".join(self._chunks), dtype=np.int16).astype(np.float32)
        return float(np.sqrt(np.mean(arr ** 2))) / 32768.0


class CameraStream:
    """Lit le flux RTSP d'une caméra et distribue les chunks PCM aux abonnés."""

    def __init__(self, camera: CameraConfig):
        self.camera = camera
        self._rms_buf = RMSBuffer()
        self._subscribers: list[asyncio.Queue] = []

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=400)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self._subscribers:
            self._subscribers.remove(q)

    @property
    def rms(self) -> float:
        return self._rms_buf.rms()

    async def run(self):
        while True:
            proc = None
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-loglevel", "quiet",
                    "-rtsp_transport", "tcp",
                    "-i", self.camera.rtsp_url,
                    "-vn", "-ar", str(RATE), "-ac", "1",
                    "-f", "s16le", "pipe:1",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                while True:
                    data = await proc.stdout.read(CHUNK_BYTES)
                    if not data:
                        break
                    self._rms_buf.push(data)
                    for q in list(self._subscribers):
                        try:
                            q.put_nowait(data)
                        except asyncio.QueueFull:
                            pass
            except Exception as e:
                log.error("[%s] Stream error: %s", self.camera.name, e)
            finally:
                if proc:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            log.info("[%s] Stream lost, retry in 3s", self.camera.name)
            await asyncio.sleep(3)


class WakeWordWatcher:
    """Connecte un stream caméra à openWakeWord et signale les détections."""

    def __init__(
        self,
        stream: CameraStream,
        wake_uri: str,
        wake_word: str,
        on_detect,
        all_streams: dict[str, CameraStream],
    ):
        self.stream = stream
        self.wake_uri = wake_uri
        self.wake_word = wake_word.lower()
        self.on_detect = on_detect
        self.all_streams = all_streams
        self._last_detect = 0.0

    async def _send_audio(self, client: WyomingClient, audio_q: asyncio.Queue):
        await client.send("audio-start", {"rate": RATE, "width": WIDTH, "channels": CHANNELS})
        while True:
            chunk = await audio_q.get()
            await client.send(
                "audio-chunk",
                {"rate": RATE, "width": WIDTH, "channels": CHANNELS},
                payload=chunk,
            )

    async def _recv_detections(self, client: WyomingClient):
        while True:
            evt = await client.recv()
            if evt.get("type") != "detection":
                continue

            data = evt.get("data", {})
            name = str(data.get("name", "")).lower()
            score = float(data.get("score", 1.0))

            if self.wake_word not in name:
                continue

            now = time.monotonic()
            if now - self._last_detect < PIPELINE_DEBOUNCE_S:
                continue
            self._last_detect = now

            log.info("[%s] Wake word! score=%.2f rms=%.3f", self.stream.camera.name, score, self.stream.rms)

            event = WakeEvent(
                camera=self.stream.camera,
                ww_score=score,
                rms=self.stream.rms,
                timestamp=now,
                all_scores={self.stream.camera.name: score},
                all_rms={n: s.rms for n, s in self.all_streams.items()},
            )
            asyncio.ensure_future(self.on_detect(event))

    async def run(self):
        while True:
            audio_q = None
            try:
                async with WyomingClient(self.wake_uri) as client:
                    audio_q = self.stream.subscribe()
                    await asyncio.gather(
                        self._send_audio(client, audio_q),
                        self._recv_detections(client),
                    )
            except Exception as e:
                log.error("[%s] WakeWord error: %s", self.stream.camera.name, e)
            finally:
                if audio_q is not None:
                    self.stream.unsubscribe(audio_q)
            await asyncio.sleep(3)


class MultiMicSatellite:
    """Orchestre tous les streams et détections de wake word."""

    def __init__(self, cameras: list[CameraConfig], config: dict):
        self.cameras = cameras
        self.config = config
        self.streams: dict[str, CameraStream] = {}
        self._pipeline_lock = asyncio.Lock()

    async def start(self):
        for cam in self.cameras:
            self.streams[cam.name] = CameraStream(cam)

        tasks = []
        for name, stream in self.streams.items():
            tasks.append(asyncio.create_task(stream.run(), name=f"stream-{name}"))
            watcher = WakeWordWatcher(
                stream=stream,
                wake_uri=self.config["wyoming_wake_uri"],
                wake_word=self.config["wake_word"],
                on_detect=self._on_wake,
                all_streams=self.streams,
            )
            tasks.append(asyncio.create_task(watcher.run(), name=f"watcher-{name}"))

        log.info("MultiMicSatellite ready — %d cameras", len(self.cameras))
        await asyncio.gather(*tasks)

    async def _on_wake(self, event: WakeEvent):
        if self._pipeline_lock.locked():
            log.info("Pipeline occupé, wake ignoré depuis %s", event.camera.name)
            return
        async with self._pipeline_lock:
            try:
                from pipeline import run_full_pipeline
                await run_full_pipeline(
                    wake_event=event,
                    streams=self.streams,
                    config=self.config,
                )
            except Exception as e:
                log.error("Pipeline error: %s", e, exc_info=True)
