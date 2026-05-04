#!/usr/bin/env python3
"""Quick test: login → get WSS token → handshake → (WebRTC audio later)."""
import asyncio
import sys
import logging

logging.basicConfig(level=logging.DEBUG)
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from auth import ArentiSession
from mts import MTSSession

SALON_DEVICE_ID = "10000730080"
SALON_DEVICE_CODE = "ppsc8c8779830131445a"


async def main(username: str, password: str):
    sess = ArentiSession(username, password)
    print("[1] Login...")
    await sess.login()
    print(f"    userId={sess.user_id}  token={sess.user_token[:20]}...")

    mts = MTSSession(sess, SALON_DEVICE_ID, SALON_DEVICE_CODE)

    print("[2] Fetching device hostKey...")
    await mts.get_host_key()
    print(f"    hostKey={mts._host_key}")

    print("[3] Waking up camera on cloud...")
    await mts.wake_up()

    print("[4] Getting WSS token...")
    await mts.get_wss_token()
    print(f"    accessid={mts._accessid[:30]}...")

    print("[4] Connecting WebSocket...")
    await mts.connect()
    print("    Connected!")

    print("[5] MTS handshake (hello + option)...")
    await mts.handshake()
    print(f"    TURN servers: {mts.ice_servers}")

    mode = sys.argv[3] if len(sys.argv) > 3 else "file"
    if mode == "tone":
        print("[6] WebRTC talk — 1kHz sine tone test (3s)...")
        from webrtc_talk import talk_tone
        await talk_tone(mts, freq_hz=1000.0, duration_s=3.0)
    else:
        print("[6] WebRTC talk with ding.mp3...")
        from webrtc_talk import talk_file
        ding = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ding.mp3")
        await talk_file(mts, ding)

    print("[OK] Done!")
    await sess.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 test_talk.py <email> <password> [tone|file]")
        sys.exit(1)
    asyncio.run(main(sys.argv[1], sys.argv[2]))
