"""Wyoming TTS client — connects to any Wyoming TTS server (Piper, etc.) and returns PCM."""
import asyncio
import json
import logging
from typing import AsyncIterator

import numpy as np

log = logging.getLogger(__name__)

TARGET_RATE = 8000  # camera expects 8kHz PCMU


async def _parse_uri(uri: str) -> tuple[str, int]:
    """Parse wyoming+tcp://host:port or tcp://host:port or host:port."""
    uri = uri.replace("wyoming+tcp://", "").replace("tcp://", "")
    host, port_str = uri.rsplit(":", 1)
    return host, int(port_str)


async def _read_event(reader: asyncio.StreamReader) -> dict:
    """Read one Wyoming event: JSON header line + optional data + optional payload."""
    line = await reader.readline()
    if not line:
        raise EOFError("Wyoming connection closed")
    header = json.loads(line.decode())
    # Read extra JSON data block if present
    data_len = header.get("data_length", 0)
    if data_len:
        data_bytes = await reader.readexactly(data_len)
        header["data"] = json.loads(data_bytes.decode())
    # Read binary payload if present
    payload_len = header.get("payload_length", 0)
    if payload_len:
        header["_payload"] = await reader.readexactly(payload_len)
    return header


async def _write_event(writer: asyncio.StreamWriter, type_: str, data: dict | None = None) -> None:
    """Write one Wyoming event with proper data_length encoding."""
    header: dict = {"type": type_}
    if data:
        data_bytes = json.dumps(data).encode()
        header["data_length"] = len(data_bytes)
        line = json.dumps(header) + "\n"
        writer.write(line.encode() + data_bytes)
    else:
        line = json.dumps(header) + "\n"
        writer.write(line.encode())
    await writer.drain()


async def synthesize_to_pcm(
    uri: str,
    text: str,
    voice: str | None = None,
    language: str = "fr",
) -> bytes:
    """
    Call Wyoming TTS server, return raw s16le PCM resampled to TARGET_RATE.
    uri: e.g. "wyoming+tcp://piper:10200" or "192.168.1.149:10200"
    """
    host, port = await _parse_uri(uri)
    log.info("Wyoming TTS connect %s:%d text=%r voice=%s", host, port, text[:40], voice)

    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=10
    )
    try:
        voice_data: dict = {"language": language}
        if voice:
            voice_data["name"] = voice

        await _write_event(writer, "synthesize", {
            "text": text,
            "voice": voice_data,
        })

        source_rate: int | None = None
        source_width: int | None = None
        pcm_buf = bytearray()

        while True:
            event = await asyncio.wait_for(_read_event(reader), timeout=30)
            etype = event.get("type")

            if etype == "audio-start":
                d = event.get("data", {})
                source_rate = d.get("rate", 22050)
                source_width = d.get("width", 2)
                log.info("Wyoming audio-start rate=%d width=%d", source_rate, source_width)

            elif etype == "audio-chunk":
                payload = event.get("_payload", b"")
                if payload:
                    pcm_buf.extend(payload)

            elif etype == "audio-stop":
                log.info("Wyoming audio-stop, received %d bytes PCM", len(pcm_buf))
                break

            elif etype == "error":
                raise RuntimeError(f"Wyoming TTS error: {event.get('data')}")

        if not pcm_buf:
            raise RuntimeError("Wyoming TTS returned no audio")

        # Resample to TARGET_RATE if needed
        if source_rate and source_rate != TARGET_RATE:
            arr = np.frombuffer(bytes(pcm_buf), dtype=np.int16).astype(np.float32)
            n_out = int(len(arr) * TARGET_RATE / source_rate)
            indices = np.linspace(0, len(arr) - 1, n_out)
            arr = np.interp(indices, np.arange(len(arr)), arr)
            return arr.astype(np.int16).tobytes()

        return bytes(pcm_buf)

    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
