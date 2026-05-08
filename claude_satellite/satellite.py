"""Multi-mic satellite: surveille tous les streams caméras pour le wake word."""
import asyncio
import concurrent.futures
import logging
import time
from dataclasses import dataclass, field

import numpy as np

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

    async def _log_stderr(self, proc):
        async for line in proc.stderr:
            txt = line.decode().strip()
            if txt:
                log.warning("[%s] FFmpeg: %s", self.camera.name, txt)

    async def run(self):
        while True:
            proc = None
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-loglevel", "warning",
                    "-rtsp_transport", "tcp",
                    "-i", self.camera.rtsp_url,
                    "-vn", "-ar", str(RATE), "-ac", "1",
                    "-f", "s16le", "pipe:1",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                asyncio.ensure_future(self._log_stderr(proc))
                log.info("[%s] FFmpeg démarré (pid=%d)", self.camera.name, proc.pid)
                first = True
                while True:
                    data = await proc.stdout.read(CHUNK_BYTES)
                    if not data:
                        break
                    if first:
                        log.info("[%s] Premier chunk audio reçu ✓", self.camera.name)
                        first = False
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


class LocalWakeWordDetector:
    """Détection de wake word locale via openwakeword Python lib (sans Wyoming)."""

    OWW_CHUNK = 1280  # 80ms à 16000Hz — taille standard openwakeword

    def __init__(self, streams: dict[str, "CameraStream"], wake_word: str, on_detect, threshold: float = 0.5):
        self.streams = streams
        self.wake_word = wake_word.lower()
        self.on_detect = on_detect
        self.threshold = threshold
        self._last_detect = 0.0

    async def run(self):
        from openwakeword.model import Model

        loop = asyncio.get_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        log.info("LocalWakeWord: chargement modèle '%s'...", self.wake_word)
        model = await loop.run_in_executor(executor, lambda: Model(wakeword_models=[self.wake_word]))
        log.info("LocalWakeWord: modèle '%s' prêt ✓", self.wake_word)

        queues = {name: stream.subscribe() for name, stream in self.streams.items()}
        buffers: dict[str, bytearray] = {name: bytearray() for name in self.streams}
        n_chunks = 0

        try:
            while True:
                # Drainer toutes les queues dans les buffers
                got = False
                for name, q in queues.items():
                    while True:
                        try:
                            buffers[name].extend(q.get_nowait())
                            got = True
                        except asyncio.QueueEmpty:
                            break

                # Trouver la caméra avec le plus de données et la plus forte RMS
                ready = [n for n, b in buffers.items() if len(b) >= self.OWW_CHUNK * 2]
                if not ready:
                    await asyncio.sleep(0.02)
                    continue

                src = max(ready, key=lambda n: self.streams[n].rms)
                audio_bytes = bytes(buffers[src][:self.OWW_CHUNK * 2])
                del buffers[src][:self.OWW_CHUNK * 2]

                audio = np.frombuffer(audio_bytes, dtype=np.int16)
                predictions = await loop.run_in_executor(executor, model.predict, audio)

                n_chunks += 1
                if n_chunks % 125 == 0:  # ~10s
                    scores = {k: f"{v:.3f}" for k, v in predictions.items()}
                    log.info("LocalWW: src=%s rms=%.4f scores=%s", src, self.streams[src].rms, scores)

                for ww_name, score in predictions.items():
                    score = float(score)
                    if score < self.threshold:
                        continue
                    now = time.monotonic()
                    if now - self._last_detect < PIPELINE_DEBOUNCE_S:
                        continue
                    self._last_detect = now

                    best = max(self.streams, key=lambda n: self.streams[n].rms)
                    best_stream = self.streams[best]
                    log.info("Wake word! name=%s score=%.2f source=%s rms=%.3f",
                             ww_name, score, best, best_stream.rms)

                    event = WakeEvent(
                        camera=best_stream.camera,
                        ww_score=score,
                        rms=best_stream.rms,
                        timestamp=now,
                        all_scores={best: score},
                        all_rms={n: s.rms for n, s in self.streams.items()},
                    )
                    asyncio.ensure_future(self.on_detect(event))

        except Exception as e:
            log.error("LocalWakeWord error: %s", e, exc_info=True)
        finally:
            for name, q in queues.items():
                self.streams[name].unsubscribe(q)
            executor.shutdown(wait=False)


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

        detector = LocalWakeWordDetector(
            streams=self.streams,
            wake_word=self.config["wake_word"],
            on_detect=self._on_wake,
            threshold=float(self.config.get("wake_word_threshold", 0.5)),
        )
        tasks.append(asyncio.create_task(detector.run(), name="local-wakeword"))

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
