"""Meari/Arenti authentication: login + HMAC sign."""
import hashlib
import hmac
import time
import random
import base64
import json
import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
from cryptography.hazmat.backends import default_backend

ARENTI_BASE = "https://web-eu.arenti.net"
RSA_PUBLIC_KEY_PEM = """\
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCS3LSuG7ttWGvFV+Cn6FCKqqMx
e9kF81McQO+tc4H5n1FImoeDDM28z1mnGGSqJHNAUbiRzcHYL8VAblH7Lbo7SwDn
Qtm+gjRIl9yUuyLBlA39ry14+dqCEXaO9N4hNOeRbZUXxTB126DkvKQOxzfoU1/m
nDji0gUCy/zcB1KNOwIDAQAB
-----END PUBLIC KEY-----"""
RSA_SALT = "https://www.mearitek.com/zh/home-cn/"
APP_ID = "39"
SIGN_VER = "1.1"

_public_key = serialization.load_pem_public_key(
    RSA_PUBLIC_KEY_PEM.encode(), backend=default_backend()
)


def _rsa_encrypt_password(password: str) -> str:
    plaintext = (password + RSA_SALT).encode("utf-8")
    ciphertext = _public_key.encrypt(plaintext, asym_padding.PKCS1v15())
    # JS: base64encode(jsencrypt.encrypt(plaintext))
    # JSEncrypt.encrypt() already returns base64, then base64encode() wraps again → double b64
    inner = base64.b64encode(ciphertext).decode()
    return base64.b64encode(inner.encode()).decode()


def _nonce() -> str:
    # JS: MD5(randomNmber()) — nonce is an MD5 hex string
    rand = f"{int(random.random() * 1e7):x}_{int(time.time() * 1000)}_{str(random.random())[2:7]}"
    return hashlib.md5(rand.encode()).hexdigest()


def _md5_lower(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()  # lowercase, as CryptoJS returns


def _sign(key: str, user_id: str, t: str, nonce: str, query_string: str, body_bytes: bytes) -> str:
    body_md5 = _md5_lower(body_bytes) if body_bytes else ""
    message = user_id + t + nonce + query_string + body_md5
    return hmac.new(key.encode(), message.encode(), hashlib.sha256).hexdigest().upper()


def _base_headers(t: str, nonce: str, sign: str, identity: str = "-1") -> dict:
    return {
        "identity": identity,
        "t": t,
        "nonce": nonce,
        "sign": sign,
        "signVer": SIGN_VER,
        "app": APP_ID,
        "Content-Type": "application/json",
    }


class ArentiSession:
    def __init__(self, username: str, password: str, country_code: str = "FR", phone_code: str = "33"):
        self.username = username
        self.password = password
        self.country_code = country_code
        self.phone_code = phone_code
        self.user_token: str = "-"
        self.user_id: str = "-1"
        self.http_domain: str = ARENTI_BASE
        self._client = httpx.AsyncClient(timeout=15)

    def _sign_headers(self, body_bytes: bytes, params: dict | None = None) -> tuple[dict, str, str]:
        t = str(int(time.time() * 1000))
        nonce = _nonce()
        qs = ""
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
        sign = _sign(self.user_token, self.user_id, t, nonce, qs, body_bytes)
        return _base_headers(t, nonce, sign, self.user_id), t, nonce

    async def _get_http_domain(self) -> None:
        """Call /redirect to get the regional httpDomain."""
        body = {"userAccount": self.username, "countryCode": self.country_code, "sourceApp": APP_ID}
        body_bytes = json.dumps(body, separators=(",", ":")).encode()
        headers, _, _ = self._sign_headers(body_bytes)
        url = f"{ARENTI_BASE}/ipc_web/redirect"
        resp = await self._client.post(url, content=body_bytes, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        domain = data.get("data") or ""
        if isinstance(domain, str) and domain.startswith("http"):
            self.http_domain = domain.rstrip("/")

    async def login(self) -> None:
        await self._get_http_domain()
        enc_pw = _rsa_encrypt_password(self.password)
        body = {
            "userAccount": self.username,
            "password": enc_pw,
            "phoneCode": self.phone_code,
            "countryCode": self.country_code,
            "lngType": "en",
        }
        body_bytes = json.dumps(body, separators=(",", ":")).encode()
        headers, _, _ = self._sign_headers(body_bytes)
        url = f"{self.http_domain}/ipc_web/user/login"
        resp = await self._client.post(url, content=body_bytes, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") not in (200, "200", 1001, "1001"):
            raise RuntimeError(f"Login failed: {data}")
        info = data["data"]
        self.user_token = info["userToken"]
        self.user_id = str(info.get("userID") or info.get("userId"))
        # Use the regional httpDomain for all subsequent API calls
        if info.get("httpDomain"):
            self.http_domain = info["httpDomain"].rstrip("/")

    async def get(self, path: str, params: dict | None = None) -> dict:
        t = str(int(time.time() * 1000))
        nonce = _nonce()
        qs = "&".join(f"{k}={v}" for k, v in (params or {}).items())
        sign = _sign(self.user_token, self.user_id, t, nonce, qs, b"")
        headers = _base_headers(t, nonce, sign, self.user_id)
        # device/list uses web-eu.arenti.net, not openapi domain
        base = "https://web-eu.arenti.net"
        resp = await self._client.get(f"{base}/ipc_web{path}", params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def post(self, path: str, body: dict) -> dict:
        body_bytes = json.dumps(body, separators=(",", ":")).encode()
        t = str(int(time.time() * 1000))
        nonce = _nonce()
        sign = _sign(self.user_token, self.user_id, t, nonce, "", body_bytes)
        headers = _base_headers(t, nonce, sign, self.user_id)
        base = "https://web-eu.arenti.net"
        resp = await self._client.post(f"{base}/ipc_web{path}", content=body_bytes, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def get_device_host_key(self, sn_num: str) -> str:
        """Return the hostKey for a device (used as devicecode in MTS offer)."""
        data = await self.get("/device/list", params={"userId": self.user_id})
        for ipc in (data.get("data") or {}).get("ipc", []):
            if ipc.get("snNum") == sn_num:
                return ipc.get("hostKey", "")
        return ""

    async def query_device_status(self, sn_nums: list[str]) -> dict:
        """Wake up cameras on cloud + get their online status."""
        data = await self.post("/iot/query_device_status", {
            "iotHost": "https://openapi-eu.mearicloud.com",
            "snNumList": sn_nums,
        })
        return {d["deviceid"]: d for d in (data.get("data") or [])}

    async def close(self) -> None:
        await self._client.aclose()
