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

    def snapshot(self) -> bytes:
        return b"".join(self._chunks)


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

    def recent_audio(self) -> bytes:
        """Dernières ~2s d'audio (pour pré-remplir le buffer pipeline)."""
        return self._rms_buf.snapshot()

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
    """Lit un event Wyoming depuis un StreamReader (protocole ≥1.8 compatible)."""
    line = await reader.readline()
    if not line:
        raise EOFError("OWW connection closed")
    line_str = line.decode().strip()
    try:
        header = json.loads(line_str)
    except json.JSONDecodeError:
        header, _ = json.JSONDecoder().raw_decode(line_str)
    # Wyoming ≥1.8 : data envoyé comme blob JSON séparé
    data_len = header.get("data_length", 0)
    if data_len:
        data_blob = await reader.readexactly(data_len)
        header["data"] = json.loads(data_blob.decode())
    payload = b""
    if header.get("payload_length", 0) > 0:
        payload = await reader.readexactly(header["payload_length"])
    return header["type"], header.get("data", {}), payload


class WyomingWakeWordDetector:
    """Détection de wake word via Wyoming OWW — une connexion par caméra."""

    CHUNK_BYTES = int(RATE * WIDTH * CHANNELS * 80 / 1000)  # 80ms = 2560 bytes (taille native OWW)

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
        writer.write(_wyoming_encode("detect", {}))
        writer.write(_wyoming_encode("audio-start", {
            "rate": RATE, "width": WIDTH, "channels": CHANNELS,
        }))
        await writer.drain()
        return reader, writer

    async def _camera_loop(self, cam_name: str, stream: "CameraStream"):
        """Boucle dédiée à UNE caméra : audio continu → OWW → detection."""
        queue = stream.subscribe()
        n = 0
        try:
            while True:
                try:
                    reader, writer = await self._connect()
                    log.info("OWW [%s]: connecté", cam_name)
                except Exception as e:
                    log.error("OWW [%s]: connexion impossible: %s — retry 5s", cam_name, e)
                    await asyncio.sleep(5)
                    continue

                # Vider la queue des vieux chunks avant de commencer
                drained = 0
                while True:
                    try:
                        queue.get_nowait()
                        drained += 1
                    except asyncio.QueueEmpty:
                        break
                if drained:
                    log.debug("OWW [%s]: %d vieux chunks vidés", cam_name, drained)

                buf = bytearray()
                reader_task = asyncio.create_task(
                    self._reader_loop(cam_name, reader), name=f"oww-reader-{cam_name}"
                )
                try:
                    while not reader_task.done():
                        try:
                            buf.extend(queue.get_nowait())
                        except asyncio.QueueEmpty:
                            await asyncio.sleep(0.005)
                            continue

                        while len(buf) >= self.CHUNK_BYTES:
                            chunk = bytes(buf[:self.CHUNK_BYTES])
                            del buf[:self.CHUNK_BYTES]
                            writer.write(_wyoming_encode("audio-chunk", {
                                "rate": RATE, "width": WIDTH, "channels": CHANNELS,
                            }, chunk))
                            n += 1
                            if n % 125 == 0:  # ~10s
                                log.info("OWW [%s]: rms=%.4f chunks=%d", cam_name, stream.rms, n)
                        await writer.drain()
                except Exception as e:
                    log.warning("OWW [%s]: write error: %s", cam_name, e)

                reader_task.cancel()
                try:
                    if not writer.is_closing():
                        writer.write(_wyoming_encode("audio-stop", {}))
                        await writer.drain()
                        writer.close()
                except Exception:
                    pass
                log.info("OWW [%s]: déconnecté, retry 3s", cam_name)
                await asyncio.sleep(3)
        finally:
            stream.unsubscribe(queue)

    async def _reader_loop(self, cam_name: str, reader: asyncio.StreamReader):
        """Lit les events OWW pour une caméra."""
        while True:
            try:
                evt_type, data, _ = await _wyoming_read_event(reader)
            except Exception as e:
                log.warning("OWW [%s]: reader error: %s", cam_name, e)
                return

            if evt_type == "detection":
                now = time.monotonic()
                if now - self._last_detect < self.debounce:
                    log.info("OWW [%s]: detection ignorée (debounce)", cam_name)
                    continue
                self._last_detect = now
                score = float(data.get("score", 1.0))
                log.info("Wake word! cam=%s name=%s score=%.2f rms=%.3f",
                         cam_name, data.get("name"), score, self.streams[cam_name].rms)
                event = WakeEvent(
                    camera=self.streams[cam_name].camera,
                    ww_score=score,
                    rms=self.streams[cam_name].rms,
                    timestamp=now,
                    all_scores={cam_name: score},
                    all_rms={n: s.rms for n, s in self.streams.items()},
                )
                asyncio.ensure_future(self.on_detect(event))
            else:
                log.info("OWW [%s]: event=%s data=%s", cam_name, evt_type, data)

    async def run(self):
        tasks = [
            asyncio.create_task(self._camera_loop(name, stream), name=f"oww-{name}")
            for name, stream in self.streams.items()
        ]
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            log.error("WyomingWakeWord error: %s", e, exc_info=True)
        finally:
            for t in tasks:
                t.cancel()


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
                # Injecter les caméras découvertes si config ne les a pas
                config = dict(self.config)
                if not config.get("cameras"):
                    config["cameras"] = [
                        {"name": c.name, "room": c.room,
                         "frigate_camera": c.frigate_camera,
                         "talkback_camera": c.talkback_camera}
                        for c in self.cameras
                    ]
                await run_full_pipeline(
                    wake_event=event,
                    streams=self.streams,
                    config=config,
                )
            except Exception as e:
                log.error("Pipeline error: %s", e, exc_info=True)

