"""Wyoming protocol client — format standard (data inline dans le header)."""
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
    # Format standard Wyoming : data inline dans le header JSON
    header: dict = {"type": type_}
    if data:
        header["data"] = data
    if payload:
        header["payload_length"] = len(payload)
    writer.write((json.dumps(header) + "\n").encode())
    if payload:
        writer.write(payload)
    await writer.drain()


class WyomingClient:
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
