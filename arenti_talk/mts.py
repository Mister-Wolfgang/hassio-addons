"""Meari MTS WebSocket + WebRTC session."""
import asyncio
import json
import time
import random
import string
import hashlib
import hmac
import uuid
import logging
from typing import Optional

import websockets

from auth import ArentiSession, _nonce, _sign, _base_headers
import httpx

log = logging.getLogger(__name__)

WSS_URL = "wss://wss-eu.mearicloud.com"


def _rand_hex(length: int) -> str:
    return "".join(random.choices("0123456789abcdef", k=length))


class MTSSession:
    """
    Manages one MTS session with a camera.

    Steps:
      1. get_wss_token()  → calls iot_sign/wss
      2. connect()        → opens WebSocket
      3. handshake()      → hello + option, receives TURN credentials
      4. negotiate_sdp()  → send offer, receive answer
      5. close()
    """

    def __init__(self, sess: ArentiSession, device_id: str, device_code: str):
        self.sess = sess
        self.device_id = device_id        # numeric, e.g. "10000730080" (kept for reference)
        self.device_code = device_code    # snNum, e.g. "ppsc8c8779830131445a"
        # short ID used for iot_sign/wss and MTS callee (snNum without "ppsc" prefix)
        self.short_id = device_code.replace("ppsc", "", 1)
        self._ws = None
        self._sid = str(uuid.uuid4())
        self._caller = _rand_hex(16)
        # filled by get_wss_token
        self._accessid: str = ""
        self._signature: str = ""
        self._token: str = ""
        self._expires: str = ""
        # filled by handshake option response
        self.ice_servers: list[dict] = []
        # filled by get_host_key()
        self._host_key: str = ""

    async def get_host_key(self) -> None:
        """Fetch hostKey from device/list — used as devicecode in MTS offer."""
        self._host_key = await self.sess.get_device_host_key(self.device_code)
        log.info("hostKey: %s", self._host_key)

    async def wake_up(self) -> None:
        """Call query_device_status to bring camera online on the cloud."""
        statuses = await self.sess.query_device_status([self.device_code])
        # deviceid in response is the snNum without 'ppsc' prefix
        short_id = self.device_code.replace("ppsc", "")
        status = statuses.get(short_id, {})
        log.info("Device status: %s", status.get("status", "unknown"))
        if status.get("status") != "online":
            log.warning("Camera still offline after wake-up: %s", status)

    async def get_wss_token(self) -> None:
        """Call iot_sign/wss to get accessid/signature/token."""
        self._expires = str(int(time.time() * 1000))
        params = {
            "expires": self._expires,
            "method": "mts:option",
            "deviceid": self.short_id,    # short ID (snNum without "ppsc" prefix)
            "devicecode": self.device_code,
        }
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        t = str(int(time.time() * 1000))
        n = _nonce()
        sig = _sign(self.sess.user_token, self.sess.user_id, t, n, qs, b"")
        hdrs = _base_headers(t, n, sig, self.sess.user_id)
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://web-eu.arenti.net/ipc_web/iot_sign/wss",
                params=params,
                headers=hdrs,
            )
        data = r.json()
        if data.get("code") not in (1001, "1001", 200, "200"):
            raise RuntimeError(f"iot_sign/wss failed: {data}")
        d = data["data"]
        self._accessid = d["accessid"]
        self._signature = d["signature"]
        self._token = d["token"]
        log.debug("WSS token obtained: accessid=%s", self._accessid[:20])

    async def connect(self) -> None:
        self._ws = await websockets.connect(WSS_URL, ping_interval=20)
        log.debug("WSS connected")

    def _build(self, method: str, params: Optional[dict] = None) -> str:
        msg: dict = {"sid": self._sid, "action": "req", "cmd": "mts", "method": method}
        if method != "hello":
            if params:
                msg["params"] = params
        return json.dumps(msg)

    async def _send(self, msg: str) -> None:
        await self._ws.send(msg)
        log.debug("MTS >> %s", json.loads(msg).get("method"))

    async def _recv(self, timeout: float = 10) -> dict:
        raw = await asyncio.wait_for(self._ws.recv(), timeout=timeout)
        msg = json.loads(raw)
        log.debug("MTS << %s", msg.get("method") or msg.get("cmd"))
        return msg

    async def handshake(self) -> None:
        """Send hello + option, wait for option response with TURN credentials."""
        await self._send(self._build("hello"))
        await self._send(json.dumps({
            "sid": self._sid,
            "action": "req",
            "cmd": "mts",
            "method": "option",
            "auth": {
                "accessid": self._accessid,
                "signature": self._signature,
                "token": self._token,
            },
            "params": {
                "caller": self._caller,
                "callee": self.short_id,      # short ID matches iot_sign/wss deviceid
                "devicecode": self.device_code,
                "expires": self._expires,
                "continent": "Europe",
                "country": "France",
            },
        }))

        # Wait for option response (may receive hello ack first)
        for _ in range(5):
            msg = await self._recv(timeout=15)
            if msg.get("method") == "option":
                p = msg.get("params", {})
                coturn_host = p.get("coturn_host") or p.get("coturn_ip", "")
                coturn_port = p.get("coturn_port", 3478)
                username = p.get("username", "")
                password = p.get("pwd", p.get("password", ""))
                if coturn_host:
                    self.ice_servers = [{
                        "urls": [f"turn:{coturn_host}:{coturn_port}"],
                        "username": username,
                        "credential": password,
                    }]
                log.info("MTS handshake done, TURN=%s", coturn_host)
                return
        raise RuntimeError("No option response received")

    def _settings_sid(self) -> str:
        """Generate settings.sid like "21ea_1777814222237_36281"."""
        prefix = _rand_hex(4)
        ts = str(int(time.time() * 1000))
        rand = str(random.randint(10000, 99999))
        return f"{prefix}_{ts}_{rand}"

    async def send_init_settings(self, channel: int = 0) -> None:
        """Send settings action:init to activate audio channel (enables camera speaker)."""
        await self._send(json.dumps({
            "sid": self._sid,
            "action": "req",
            "cmd": "mts",
            "method": "settings",
            "params": {
                "caller": self._caller,
                "callee": self.short_id,
                "settings": {
                    "sid": self._settings_sid(),
                    "method": "preview",
                    "action": "init",
                    "params": [{"channel": channel, "audio": True, "talk": True, "playerId": f"remote-player-{channel}"}],
                },
            },
        }))
        log.debug("Sent settings action:init for channel %d (with talk:True)", channel)

    async def send_play_settings(self, channel: int = 0, stream: int = 0) -> None:
        """Send settings action:play to start the preview stream on the camera."""
        await self._send(json.dumps({
            "sid": self._sid,
            "action": "req",
            "cmd": "mts",
            "method": "settings",
            "params": {
                "caller": self._caller,
                "callee": self.short_id,
                "settings": {
                    "sid": self._settings_sid(),
                    "method": "preview",
                    "action": "play",
                    "params": [{"channel": channel, "stream": stream, "talk": True}],
                },
            },
        }))
        log.debug("Sent settings action:play for channel %d stream %d (with talk:True)", channel, stream)

    async def send_talk_settings(self, channel: int = 0) -> None:
        """Send settings to activate talkback (viewer→camera speaker)."""
        await self._send(json.dumps({
            "sid": self._sid,
            "action": "req",
            "cmd": "mts",
            "method": "settings",
            "params": {
                "caller": self._caller,
                "callee": self.short_id,
                "settings": {
                    "sid": self._settings_sid(),
                    "method": "talk",
                    "action": "start",
                    "params": [{"channel": channel}],
                },
            },
        }))
        log.info("Sent settings method:talk action:start for channel %d", channel)

    async def send_volume_settings(self, channel: int = 0, volume: int = 100) -> None:
        """Send speaker volume command (0-100)."""
        await self._send(json.dumps({
            "sid": self._sid,
            "action": "req",
            "cmd": "mts",
            "method": "settings",
            "params": {
                "caller": self._caller,
                "callee": self.short_id,
                "settings": {
                    "sid": self._settings_sid(),
                    "method": "volume",
                    "action": "set",
                    "params": [{"channel": channel, "volume": volume, "type": "speaker"}],
                },
            },
        }))
        log.info("Sent volume settings: channel=%d volume=%d", channel, volume)

    async def send_preview_settings(self, channel: int = 0, stream: int = 0) -> None:
        """Send settings/preview to tell camera which stream to open before SDP."""
        settings_sid = self._settings_sid()
        msg = json.dumps({
            "sid": self._sid,
            "action": "req",
            "cmd": "mts",
            "method": "settings",
            "params": {
                "caller": self._caller,
                "callee": self.short_id,
                "settings": {
                    "sid": settings_sid,
                    "method": "preview",
                    "streams": [{"channel": channel, "stop": 0, "stream": stream}],
                },
            },
        })
        await self._send(msg)

    async def send_sdp_offer(self, sdp: str, channel: int = 0, stream: int = 0) -> str:
        """Send SDP offer, return SDP answer.

        Uses method "offer" (not "option"). The camera replies with
        method "answer". Intervening "settings" notifications are echoed back.
        """
        # Browser sends settings={"method":"preview"} only — no streams
        # Browser uses hostKey as devicecode (not snNum)
        device_code = self._host_key or self.device_code
        await self._send(json.dumps({
            "sid": self._sid,
            "action": "req",
            "cmd": "mts",
            "method": "offer",
            "params": {
                "caller": self._caller,
                "callee": self.short_id,
                "devicecode": device_code,
                "sdp": sdp,
                "settings": {"method": "preview"},
            },
        }))
        for _ in range(20):
            msg = await self._recv(timeout=15)
            method = msg.get("method")
            action = msg.get("action")
            params = msg.get("params", {})
            log.debug("SDP wait: method=%s action=%s params_keys=%s",
                      method, action, list(params.keys()))

            if method == "settings" and action == "rsp":
                echo = dict(msg)
                echo["action"] = "req"
                await self._send(json.dumps(echo))
                log.debug("Echoed settings notification back to camera")
                continue

            if method == "answer":
                if "sdp" in params:
                    return params["sdp"]
                raise RuntimeError(f"Answer missing SDP: {msg}")

            if "errid" in msg or msg.get("errstr"):
                raise RuntimeError(f"SDP offer rejected: {msg}")

        raise RuntimeError("No SDP answer received")

    async def send_candidate(self, candidate: str, sdp_mid: str, mline_index: int) -> None:
        await self._send(json.dumps({
            "sid": self._sid,
            "action": "req",
            "cmd": "mts",
            "method": "candidate",
            "params": {
                "caller": self._caller,
                "callee": self.short_id,
                "candidate": candidate,
                "sdpMid": sdp_mid,
                "sdpMLineIndex": mline_index,
            },
        }))

    async def send_settings_raw(self, settings: dict) -> None:
        """Send a raw settings payload over MTS."""
        await self._send(json.dumps({
            "sid": self._sid,
            "action": "req",
            "cmd": "mts",
            "method": "settings",
            "params": {
                "caller": self._caller,
                "callee": self.short_id,
                "settings": settings,
            },
        }))

    async def close(self) -> None:
        try:
            await self._send(json.dumps({
                "sid": self._sid,
                "action": "req",
                "cmd": "mts",
                "method": "disconnected",
                "params": {"caller": self._caller, "callee": self.short_id},
            }))
        except Exception:
            pass
        if self._ws:
            await self._ws.close()
