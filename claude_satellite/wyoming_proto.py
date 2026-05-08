"""Wyoming protocol client — same encoding as arenti_talk/wyoming_tts.py."""
import asyncio
import json
import logging
from typing import Optional

log = logging.getLogger(__name__)


def parse_uri(uri: str) -> tuple[str, int]:
    uri = uri.replace("wyoming+tcp://", "").replace("tcp://", "")
    host, port_str = uri.rsplit(":", 1)
    return host, int(port_str)


async def read_event(reader: asyncio.StreamReader) -> dict:
    line = await reader.readline()
    if not line:
        raise EOFError("Wyoming connection closed")
    header = json.loads(line.decode())
    data_len = header.get("data_length", 0)
    if data_len:
        data_bytes = await reader.readexactly(data_len)
        header["data"] = json.loads(data_bytes.decode())
    payload_len = header.get("payload_length", 0)
    if payload_len:
        header["_payload"] = await reader.readexactly(payload_len)
    return header


async def write_event(
    writer: asyncio.StreamWriter,
    type_: str,
    data: dict | None = None,
    payload: bytes | None = None,
) -> None:
    header: dict = {"type": type_}
    data_bytes = b""
    if data:
        data_bytes = json.dumps(data).encode()
        header["data_length"] = len(data_bytes)
    if payload:
        header["payload_length"] = len(payload)
    line = (json.dumps(header) + "\n").encode()
    writer.write(line)
    if data_bytes:
        writer.write(data_bytes)
    if payload:
        writer.write(payload)
    await writer.drain()


class WyomingClient:
    """Async context manager wrapping a Wyoming TCP connection."""

    def __init__(self, uri: str):
        self.host, self.port = parse_uri(uri)
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def __aenter__(self) -> "WyomingClient":
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), timeout=10
        )
        return self

    async def __aexit__(self, *_):
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

    async def send(self, type_: str, data: dict | None = None, payload: bytes | None = None):
        await write_event(self._writer, type_, data, payload)

    async def recv(self) -> dict:
        return await read_event(self._reader)
