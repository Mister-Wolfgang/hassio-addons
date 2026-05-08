"""Entry point FastAPI — satellite vocal + login OAuth pour keyring Claude."""
import asyncio
import json
import logging
import os
import re
import subprocess
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse

from satellite import MultiMicSatellite, CameraConfig

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

OPTIONS_PATH = os.environ.get("OPTIONS_PATH", "/data/options.json")
_satellite: MultiMicSatellite | None = None
_login_pty_fd: int | None = None


def _load_config() -> dict:
    with open(OPTIONS_PATH) as f:
        return json.load(f)


async def _get_frigate_rtsp_base(ha_headers: dict) -> str | None:
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(
                "http://supervisor/core/api/config/config_entries/entry",
                headers=ha_headers, timeout=10,
            )
            r.raise_for_status()
            entries = r.json()
    except Exception as e:
        log.warning("Impossible de lire les config_entries HA: %s", e)
        return None
    for entry in entries:
        if entry.get("domain") != "frigate":
            continue
        host = entry.get("title", "").split(":")[0]
        if host:
            rtsp_base = f"rtsp://{host}:8554"
            log.info("Frigate détecté: %s → RTSP base: %s", entry["title"], rtsp_base)
            return rtsp_base
    return None


async def _discover_cameras(go2rtc_rtsp: str) -> list[CameraConfig]:
    ha_token = os.environ.get("SUPERVISOR_TOKEN", "")
    headers = {"Authorization": f"Bearer {ha_token}"}
    rtsp_base = go2rtc_rtsp
    if not go2rtc_rtsp or go2rtc_rtsp == "rtsp://homeassistant:8554":
        discovered = await _get_frigate_rtsp_base(headers)
        if discovered:
            rtsp_base = discovered
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get("http://supervisor/core/api/states", headers=headers, timeout=10)
            r.raise_for_status()
            states = r.json()
    except Exception as e:
        log.error("Impossible de contacter l'API HA: %s", e)
        return []
    cameras = []
    for state in states:
        entity_id = state.get("entity_id", "")
        if not entity_id.startswith("camera."):
            continue
        attrs = state.get("attributes", {})
        if attrs.get("client_id") != "frigate":
            continue
        camera_name = attrs.get("camera_name") or entity_id.removeprefix("camera.")
        friendly_name = attrs.get("friendly_name", camera_name)
        cameras.append(CameraConfig(
            name=camera_name,
            room=friendly_name,
            rtsp_url=f"{rtsp_base}/{camera_name}",
            frigate_camera=camera_name,
            talkback_camera=camera_name,
        ))
        log.info("Caméra découverte: %s (%s)", camera_name, friendly_name)
    return cameras


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _satellite
    config = _load_config()
    go2rtc_rtsp = config.get("go2rtc_rtsp", "rtsp://homeassistant:8554")

    manual_cameras = config.get("cameras", [])
    if manual_cameras:
        cameras = []
        for c in manual_cameras:
            rtsp_url = c.get("rtsp_url") or f"{go2rtc_rtsp}/{c['frigate_camera']}"
            cameras.append(CameraConfig(
                name=c["name"],
                room=c.get("room", c["name"]),
                rtsp_url=rtsp_url,
                frigate_camera=c["frigate_camera"],
                talkback_camera=c.get("talkback_camera", c["frigate_camera"]),
            ))
        log.info("%d caméra(s) chargée(s) depuis la config", len(cameras))
    else:
        log.info("Auto-découverte des caméras Frigate...")
        cameras = await _discover_cameras(go2rtc_rtsp)
        if cameras:
            rtsp_host = go2rtc_rtsp.replace("rtsp://", "").split(":")[0]
            if rtsp_host not in ("homeassistant", "localhost"):
                config.setdefault("frigate_url", f"http://{rtsp_host}:5000")
        else:
            log.warning("Aucune caméra trouvée — satellite inactif")

    # Vérifier si Claude est déjà authentifié (keyring)
    r = subprocess.run(
        ["claude", "--version"],
        capture_output=True, env={**os.environ, "HOME": "/data"}, timeout=10,
    )
    if r.returncode == 0:
        log.info("Claude CLI disponible: %s", r.stdout.decode().strip())
    else:
        log.warning("Claude CLI non disponible ou non authentifié — visiter /login")

    _satellite = MultiMicSatellite(cameras=cameras, config=config)
    task = asyncio.create_task(_satellite.start())
    log.info("Claude Satellite démarré — %d caméra(s)", len(cameras))
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Claude Satellite", lifespan=lifespan)


@app.get("/health")
async def health():
    n = len(_satellite.cameras) if _satellite else 0
    return JSONResponse({"status": "ok", "cameras": n})


_LOGIN_HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<title>Claude Satellite — Login</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  body { font-family: system-ui, sans-serif; max-width: 600px; margin: 60px auto; padding: 0 20px; background: #0f1117; color: #e2e8f0; }
  h1 { font-size: 1.5rem; margin-bottom: 8px; }
  p  { color: #94a3b8; margin-bottom: 24px; }
  button { background: #6366f1; color: white; border: none; padding: 12px 28px; border-radius: 8px; font-size: 1rem; cursor: pointer; }
  button:disabled { opacity: .5; cursor: default; }
  #status { margin-top: 28px; padding: 16px; border-radius: 8px; display: none; }
  #status.waiting  { background: #1e293b; border: 1px solid #334155; }
  #status.success  { background: #14532d; border: 1px solid #16a34a; }
  #status.error    { background: #450a0a; border: 1px solid #dc2626; }
  #url-box { word-break: break-all; margin-top: 10px; }
  a { color: #818cf8; }
  .spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid #475569; border-top-color: #6366f1; border-radius: 50%; animation: spin .8s linear infinite; margin-right: 8px; vertical-align: middle; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<h1>🔐 Claude Satellite — Authentification</h1>
<p>Connecte ton compte Claude Max pour activer l'assistant vocal.<br>
<small style="color:#64748b">À faire une seule fois — les credentials sont sauvegardés.</small></p>
<button id="btn" onclick="startLogin()">Démarrer le login OAuth</button>
<div id="status"></div>
<script>
async function startLogin() {
  const btn = document.getElementById('btn');
  const box = document.getElementById('status');
  btn.disabled = true;
  box.className = 'waiting'; box.style.display = 'block';
  box.innerHTML = '<span class="spinner"></span> Démarrage…';
  const es = new EventSource('/login/stream');
  es.onmessage = function(e) {
    const d = JSON.parse(e.data);
    if (d.url) {
      box.innerHTML = '🔗 Ouvre ce lien dans ton navigateur :<br><div id="url-box"><a href="'+d.url+'" target="_blank">'+d.url+'</a></div><br><br>Puis colle le code :<br><input id="code-input" type="text" placeholder="p9kMbh..." style="width:100%;padding:8px;margin-top:8px;background:#0f1117;color:#e2e8f0;border:1px solid #475569;border-radius:6px;font-size:.9rem"><br><button onclick="submitCode()" style="margin-top:8px;background:#6366f1;color:white;border:none;padding:8px 20px;border-radius:6px;cursor:pointer">Envoyer</button><br><br><span class="spinner"></span> En attente…';
    } else if (d.status === 'ok') {
      es.close();
      box.className = 'success'; box.innerHTML = '✅ Connecté ! Credentials sauvegardés.';
      btn.textContent = 'OK'; btn.disabled = false;
    } else if (d.status === 'error') {
      es.close();
      box.className = 'error'; box.innerHTML = '❌ Erreur : ' + d.msg;
      btn.disabled = false;
    }
  };
  es.onerror = function() { es.close(); box.className='error'; box.innerHTML='❌ Connexion perdue.'; btn.disabled=false; };
}
async function submitCode() {
  const code = document.getElementById('code-input').value.trim();
  if (!code) return;
  await fetch('/login/code', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({code})});
}
</script>
</body>
</html>"""


@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return _LOGIN_HTML


@app.get("/login/stream")
async def login_stream():
    async def generator():
        import pty
        import select

        global _login_pty_fd
        env = {**os.environ, "HOME": "/data"}
        master_fd, slave_fd = pty.openpty()
        _login_pty_fd = master_fd
        proc = None
        login_ok = False
        try:
            proc = subprocess.Popen(
                ["/usr/local/bin/claude"],
                stdin=slave_fd, stdout=slave_fd, stderr=slave_fd,
                env=env, close_fds=True,
            )
            os.close(slave_fd)

            async def send_keys():
                await asyncio.sleep(1.5)
                for _ in range(3):
                    os.write(master_fd, b"\r")
                    await asyncio.sleep(0.8)
                os.write(master_fd, b"/login\r")

            key_task = asyncio.create_task(send_keys())
            buf = ""
            url_sent = False
            deadline = asyncio.get_event_loop().time() + 180

            while proc.poll() is None:
                await asyncio.sleep(0.05)
                if asyncio.get_event_loop().time() > deadline:
                    key_task.cancel()
                    proc.terminate()
                    yield f"data: {json.dumps({'status': 'error', 'msg': 'timeout'})}\n\n"
                    return
                r, _, _ = select.select([master_fd], [], [], 0)
                if not r:
                    continue
                try:
                    chunk = os.read(master_fd, 4096).decode("utf-8", errors="replace")
                except OSError:
                    break
                clean = re.sub(r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])", "", chunk)
                clean = clean.replace("\r", "")
                buf += clean
                log.info("login pty: %s", repr(clean.strip()[:200]))

                searchable = re.sub(r"[\x00-\x1f\x7f]", "", buf)
                if not url_sent:
                    m = re.search(r"https://\S{20,}", searchable)
                    if m:
                        url_sent = True
                        buf = ""
                        yield f"data: {json.dumps({'url': m.group(0).rstrip('.')})}\n\n"
                        continue

                # Succès quand les credentials sont sauvegardés (animation ***)
                if not login_ok:
                    low = searchable.lower()
                    if "*" * 20 in searchable or any(p in low for p in (
                            "login successful", "logged in", "authenticated")):
                        login_ok = True
                        key_task.cancel()
                        log.info("login: succès OAuth — credentials dans le keyring")
                        # Tuer le processus claude (on n'a plus besoin de la session TUI)
                        await asyncio.sleep(0.5)
                        proc.terminate()
                        yield f"data: {json.dumps({'status': 'ok'})}\n\n"
                        return

            key_task.cancel()
            if login_ok or proc.returncode in (0, -15):
                yield f"data: {json.dumps({'status': 'ok'})}\n\n"
            else:
                yield f"data: {json.dumps({'status': 'error', 'msg': f'exit {proc.returncode}'})}\n\n"
        except Exception as e:
            log.exception("login stream error")
            yield f"data: {json.dumps({'status': 'error', 'msg': str(e)})}\n\n"
        finally:
            _login_pty_fd = None
            try:
                os.close(master_fd)
            except OSError:
                pass
            if proc and proc.poll() is None:
                proc.terminate()

    return StreamingResponse(generator(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/login/code")
async def login_code(request: Request):
    global _login_pty_fd
    body = await request.json()
    code = (body.get("code") or "").strip()
    if not code or _login_pty_fd is None:
        return JSONResponse({"ok": False, "error": "no active login session"})
    try:
        os.write(_login_pty_fd, (code + "\r").encode())
        return JSONResponse({"ok": True})
    except OSError as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.get("/")
async def index():
    if not _satellite:
        return JSONResponse({"status": "no satellite"})
    return JSONResponse({
        "status": "running",
        "cameras": [
            {"name": s.camera.name, "room": s.camera.room, "rms": round(s.rms, 4)}
            for s in _satellite.streams.values()
        ],
    })


@app.post("/trigger/{camera_name}")
async def trigger_wake(camera_name: str):
    if not _satellite:
        return JSONResponse({"ok": False, "error": "satellite not running"})
    stream = _satellite.streams.get(camera_name)
    if not stream:
        return JSONResponse({"ok": False, "error": f"camera inconnue", "available": list(_satellite.streams.keys())})
    from satellite import WakeEvent
    import time
    event = WakeEvent(
        camera=stream.camera, ww_score=1.0, rms=stream.rms,
        timestamp=time.monotonic(),
        all_scores={camera_name: 1.0},
        all_rms={n: s.rms for n, s in _satellite.streams.items()},
    )
    asyncio.ensure_future(_satellite._on_wake(event))
    return JSONResponse({"ok": True, "camera": camera_name})
