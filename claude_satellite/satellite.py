"""Multi-mic satellite: surveille tous les streams caméras pour le wake word."""
import asyncio
import json
import logging
import struct
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


def _wyoming_encode(event_type: str, data: dict, payload: bytes = b"") -> bytes:
    """Encode un event Wyoming: header JSON + payload binaire."""
    header = json.dumps({"type": event_type, "data": data, "payload_length": len(payload)}) + "\n"
    return header.encode() + payload


async def _wyoming_read_event(reader: asyncio.StreamReader) -> tuple[str, dict, bytes]:
    """Lit un event Wyoming depuis un StreamReader."""
    line = await reader.readline()
    header = json.loads(line)
    payload = b""
    if header.get("payload_length", 0) > 0:
        payload = await reader.readexactly(header["payload_length"])
    return header["type"], header.get("data", {}), payload


class WyomingWakeWordDetector:
    """Détection de wake word via Wyoming openWakeWord (core-openwakeword:10400)."""

    CHUNK_BYTES = int(RATE * WIDTH * CHANNELS * 100 / 1000)  # 100ms = 3200 bytes

    def __init__(self, streams: dict[str, "CameraStream"], wyoming_uri: str, on_detect, debounce: float = PIPELINE_DEBOUNCE_S):
        self.streams = streams
        host, port = wyoming_uri.replace("tcp://", "").split(":")
        self.host = host
        self.port = int(port)
        self.on_detect = on_detect
        self.debounce = debounce
        self._last_detect = 0.0

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection(self.host, self.port)
        # Demander les infos du service pour savoir quels wake words sont dispo
        writer.write(_wyoming_encode("describe", {}))
        await writer.drain()
        try:
            evt_type, data, _ = await asyncio.wait_for(_wyoming_read_event(reader), timeout=3.0)
            if evt_type == "info":
                wake_models = data.get("wake", [])
                names = [m.get("name") for m in wake_models]
                log.info("Wyoming OWW info — wake words disponibles: %s", names)
        except asyncio.TimeoutError:
            log.warning("Wyoming OWW: pas de réponse à describe (timeout 3s)")

        writer.write(_wyoming_encode("audio-start", {
            "rate": RATE, "width": WIDTH, "channels": CHANNELS,
        }))
        await writer.drain()
        log.info("Wyoming OWW: connecté à %s:%d", self.host, self.port)
        return reader, writer

    async def _reader_task(self, reader: asyncio.StreamReader):
        """Lit les events OWW en continu dans sa propre task."""
        while True:
            try:
                evt_type, data, _ = await _wyoming_read_event(reader)
            except Exception as e:
                log.warning("Wyoming OWW: reader error: %s", e)
                return  # signal déconnexion

            if evt_type != "detection":
                continue

            now = time.monotonic()
            if now - self._last_detect < self.debounce:
                continue
            self._last_detect = now

            best = max(self.streams, key=lambda n: self.streams[n].rms)
            best_stream = self.streams[best]
            log.info("Wake word! name=%s score=%.2f src=%s rms=%.3f",
                     data.get("name"), data.get("score", 1.0), best, best_stream.rms)
            event = WakeEvent(
                camera=best_stream.camera,
                ww_score=float(data.get("score", 1.0)),
                rms=best_stream.rms,
                timestamp=now,
                all_scores={best: float(data.get("score", 1.0))},
                all_rms={n: s.rms for n, s in self.streams.items()},
            )
            asyncio.ensure_future(self.on_detect(event))

    async def run(self):
        queues = {name: stream.subscribe() for name, stream in self.streams.items()}
        buffers: dict[str, bytearray] = {name: bytearray() for name in self.streams}
        n_sent = 0
        reader_task: asyncio.Task | None = None

        try:
            while True:
                # (Re)connexion
                try:
                    reader, writer = await self._connect()
                except Exception as e:
                    log.error("Wyoming OWW: connexion impossible: %s — retry 5s", e)
                    await asyncio.sleep(5)
                    continue

                if reader_task:
                    reader_task.cancel()
                reader_task = asyncio.create_task(self._reader_task(reader), name="oww-reader")

                # Boucle d'envoi audio
                try:
                    while not reader_task.done():
                        for name, q in queues.items():
                            while True:
                                try:
                                    buffers[name].extend(q.get_nowait())
                                except asyncio.QueueEmpty:
                                    break

                        ready = [n for n, b in buffers.items() if len(b) >= self.CHUNK_BYTES]
                        if not ready:
                            await asyncio.sleep(0.01)
                            continue

                        src = max(ready, key=lambda n: self.streams[n].rms)
                        chunk = bytes(buffers[src][:self.CHUNK_BYTES])
                        del buffers[src][:self.CHUNK_BYTES]

                        writer.write(_wyoming_encode("audio-chunk", {
                            "rate": RATE, "width": WIDTH, "channels": CHANNELS,
                            "timestamp": int(time.monotonic() * 1000),
                        }, chunk))
                        await writer.drain()

                        n_sent += 1
                        if n_sent % 100 == 0:
                            rms = {n: f"{s.rms:.4f}" for n, s in self.streams.items()}
                            log.info("Wyoming OWW: src=%s rms=%s chunks=%d", src, rms, n_sent)

                except Exception as e:
                    log.warning("Wyoming OWW: write error: %s — reconnexion", e)

                if not writer.is_closing():
                    writer.close()
                log.info("Wyoming OWW: déconnecté, retry 3s")
                await asyncio.sleep(3)

        except Exception as e:
            log.error("WyomingWakeWord error: %s", e, exc_info=True)
        finally:
            for name, q in queues.items():
                self.streams[name].unsubscribe(q)
            if reader_task:
                reader_task.cancel()


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

        detector = WyomingWakeWordDetector(
            streams=self.streams,
            wyoming_uri=self.config.get("wyoming_wake_uri", "tcp://core-openwakeword:10400"),
            on_detect=self._on_wake,
        )
        tasks.append(asyncio.create_task(detector.run(), name="wyoming-wakeword"))

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
