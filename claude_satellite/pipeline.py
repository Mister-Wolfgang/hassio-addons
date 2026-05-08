"""Pipeline complet : capture audio → STT → contexte → Claude → TTS → talkback."""
import asyncio
import base64
import logging

import httpx
import numpy as np

from wyoming_proto import WyomingClient
from context_builder import build_context
from claude_handle import handle

log = logging.getLogger(__name__)

RATE = 16000
WIDTH = 2
CHANNELS = 1
CHUNK_MS = 100
CHUNK_BYTES = int(RATE * WIDTH * CHANNELS * CHUNK_MS / 1000)

COMMAND_TIMEOUT_S = 7.0
SILENCE_THRESHOLD = 0.015
SILENCE_DURATION_S = 0.8


# ─── STT via Whisper Wyoming ──────────────────────────────────────────────────

async def _stt(audio_data: bytes, stt_uri: str, language: str = "fr") -> str:
    async with WyomingClient(stt_uri) as c:
        await c.send("transcribe", {"language": language})
        await c.send("audio-start", {"rate": RATE, "width": WIDTH, "channels": CHANNELS})

        offset = 0
        while offset < len(audio_data):
            chunk = audio_data[offset: offset + CHUNK_BYTES]
            await c.send("audio-chunk", {"rate": RATE, "width": WIDTH, "channels": CHANNELS}, payload=chunk)
            offset += CHUNK_BYTES

        await c.send("audio-stop", {})

        for _ in range(120):
            evt = await asyncio.wait_for(c.recv(), timeout=10)
            if evt.get("type") == "transcript":
                return (evt.get("data") or {}).get("text", "").strip()

    return ""


# ─── TTS via Piper Wyoming ───────────────────────────────────────────────────

async def _tts(text: str, tts_uri: str, voice: str = "", language: str = "fr") -> bytes:
    voice_data: dict = {"language": language}
    if voice:
        voice_data["name"] = voice

    chunks = []
    async with WyomingClient(tts_uri) as c:
        await c.send("synthesize", {"text": text, "voice": voice_data})
        while True:
            evt = await asyncio.wait_for(c.recv(), timeout=20)
            t = evt.get("type")
            if t == "audio-chunk":
                payload = evt.get("_payload", b"")
                if payload:
                    chunks.append(payload)
            elif t == "audio-stop":
                break
            elif t == "error":
                log.error("TTS error: %s", evt)
                break

    return b"".join(chunks)


# ─── Capture audio commande avec VAD simple ───────────────────────────────────

async def _capture_command(audio_q: asyncio.Queue) -> bytes:
    buf = bytearray()
    silent_chunks = 0
    silent_needed = int(SILENCE_DURATION_S * 1000 / CHUNK_MS)

    try:
        async with asyncio.timeout(COMMAND_TIMEOUT_S):
            while True:
                chunk = await audio_q.get()
                if not chunk:
                    break
                buf.extend(chunk)
                arr = np.frombuffer(chunk, dtype=np.int16).astype(np.float32)
                rms = float(np.sqrt(np.mean(arr ** 2))) / 32768.0
                if rms < SILENCE_THRESHOLD:
                    silent_chunks += 1
                    if silent_chunks >= silent_needed and len(buf) > RATE * WIDTH * 0.3:
                        log.debug("VAD: silence détectée, fin de commande")
                        break
                else:
                    silent_chunks = 0
    except asyncio.TimeoutError:
        log.debug("VAD: timeout %.1fs", COMMAND_TIMEOUT_S)

    return bytes(buf)


# ─── Talkback via arenti_talk ─────────────────────────────────────────────────

async def _talkback(talkback_camera: str, audio: bytes, arenti_talk_url: str):
    if not talkback_camera or not audio:
        return
    try:
        async with httpx.AsyncClient() as c:
            await c.post(
                f"{arenti_talk_url}/play_raw",
                json={
                    "camera": talkback_camera,
                    "audio_b64": base64.standard_b64encode(audio).decode(),
                    "sample_rate": RATE,
                },
                timeout=30,
            )
    except Exception as e:
        log.warning("Talkback error [%s]: %s", talkback_camera, e)


# ─── Pipeline principal ───────────────────────────────────────────────────────

async def run_full_pipeline(wake_event, streams: dict, config: dict):
    cam = wake_event.camera
    log.info("[Pipeline] Déclenchement: %s (pièce: %s)", cam.name, cam.room)

    # 1. Capture audio commande
    audio_q = streams[cam.name].subscribe()
    try:
        audio_data = await _capture_command(audio_q)
    finally:
        streams[cam.name].unsubscribe(audio_q)

    if len(audio_data) < RATE * WIDTH * 0.2:
        log.warning("[Pipeline] Audio trop court, abandon")
        return

    # 2. STT
    log.info("[Pipeline] STT...")
    transcript = await _stt(audio_data, config["wyoming_stt_uri"], config.get("tts_language", "fr"))
    if not transcript:
        log.warning("[Pipeline] Transcript vide, abandon")
        return
    log.info("[Pipeline] Transcript: %r", transcript)

    # 3. Contexte Frigate + HA (en parallèle avec rien d'autre à faire ici)
    log.info("[Pipeline] Contexte Frigate + HA...")
    context = await build_context(
        frigate_url=config["frigate_url"],
        all_cameras=config["cameras"],
        ww_scores=wake_event.all_scores,
        rms_values=wake_event.all_rms,
    )

    # 4. Claude
    log.info("[Pipeline] Appel Claude...")
    response_text = await handle(transcript, context)
    log.info("[Pipeline] Réponse: %r", response_text)
    if not response_text:
        return

    # 5. TTS
    log.info("[Pipeline] TTS...")
    tts_audio = await _tts(
        response_text,
        config["wyoming_tts_uri"],
        voice=config.get("tts_voice", ""),
        language=config.get("tts_language", "fr"),
    )

    # 6. Talkback
    await _talkback(
        cam.talkback_camera,
        tts_audio,
        config.get("arenti_talk_url", "http://localhost:8080"),
    )
    log.info("[Pipeline] Terminé.")
