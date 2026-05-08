"""Entry point FastAPI — health check + démarrage du satellite."""
import asyncio
import json
import logging
import os
import re
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse

from satellite import MultiMicSatellite, CameraConfig

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

OPTIONS_PATH = os.environ.get("OPTIONS_PATH", "/data/options.json")
_satellite: MultiMicSatellite | None = None


def _load_config() -> dict:
    with open(OPTIONS_PATH) as f:
        return json.load(f)


async def _get_frigate_rtsp_base(ha_headers: dict) -> str | None:
    """Trouve l'URL RTSP go2rtc depuis la config de l'intégration Frigate dans HA."""
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(
                "http://supervisor/core/api/config/config_entries/entry",
                headers=ha_headers,
                timeout=10,
            )
            r.raise_for_status()
            entries = r.json()
    except Exception as e:
        log.warning("Impossible de lire les config_entries HA: %s", e)
        return None

    for entry in entries:
        if entry.get("domain") != "frigate":
            continue
        title = entry.get("title", "")
        # title = "192.168.1.131:5000" → on extrait l'hôte
        host = title.split(":")[0]
        if host:
            rtsp_base = f"rtsp://{host}:8554"
            log.info("Frigate détecté via config_entry: %s → RTSP base: %s", title, rtsp_base)
            return rtsp_base

    return None


async def _discover_cameras(go2rtc_rtsp: str) -> list[CameraConfig]:
    """Découvre automatiquement les caméras Frigate depuis les states HA."""
    ha_token = os.environ.get("SUPERVISOR_TOKEN", "")
    headers = {"Authorization": f"Bearer {ha_token}"}

    # Auto-découverte de l'URL RTSP si non configurée explicitement
    rtsp_base = go2rtc_rtsp
    if not go2rtc_rtsp or go2rtc_rtsp == "rtsp://homeassistant:8554":
        discovered = await _get_frigate_rtsp_base(headers)
        if discovered:
            rtsp_base = discovered

    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(
                "http://supervisor/core/api/states",
                headers=headers,
                timeout=10,
            )
            r.raise_for_status()
            states = r.json()
    except Exception as e:
        log.error("Impossible de contacter l'API HA pour la découverte: %s", e)
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
        rtsp_url = f"{rtsp_base}/{camera_name}"

        cameras.append(CameraConfig(
            name=camera_name,
            room=friendly_name,
            rtsp_url=rtsp_url,
            frigate_camera=camera_name,
            talkback_camera=camera_name,
        ))
        log.info("Caméra découverte: %s (%s) -> %s", camera_name, friendly_name, rtsp_url)

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
        log.info("Auto-découverte des caméras Frigate via l'API HA...")
        cameras = await _discover_cameras(go2rtc_rtsp)
        if cameras:
            # Déduire l'URL Frigate HTTP depuis la même IP que go2rtc
            rtsp_host = go2rtc_rtsp.replace("rtsp://", "").split(":")[0]
            if rtsp_host not in ("homeassistant", "localhost"):
                config.setdefault("frigate_url", f"http://{rtsp_host}:5000")
                log.info("Frigate URL: %s", config["frigate_url"])
        else:
            log.warning("Aucune caméra Frigate trouvée — satellite inactif")

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
<p>Connecte ton compte Claude Max pour activer l'assistant vocal.</p>
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
      box.innerHTML = '🔗 Ouvre ce lien dans ton navigateur :<br><div id="url-box"><a href="' + d.url + '" target="_blank">' + d.url + '</a></div><br><span class="spinner"></span> En attente de confirmation…';
    } else if (d.status === 'ok') {
      es.close();
      box.className = 'success'; box.innerHTML = '✅ Connecté ! L\'assistant vocal est actif.';
      btn.textContent = 'Reconnecté ✓'; btn.disabled = false;
    } else if (d.status === 'error') {
      es.close();
      box.className = 'error'; box.innerHTML = '❌ Erreur : ' + d.msg;
      btn.disabled = false;
    }
  };
  es.onerror = function() {
    es.close();
    box.className = 'error'; box.innerHTML = '❌ Connexion perdue.';
    btn.disabled = false;
  };
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
        env = {**os.environ, "HOME": "/data"}
        try:
            proc = await asyncio.create_subprocess_exec(
                "/usr/local/bin/claude", "login",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )
            url_sent = False
            while True:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=120)
                if not line:
                    break
                text = line.decode(errors="replace").strip()
                log.info("claude login: %s", text)
                if not url_sent:
                    m = re.search(r"https://[^\s]+", text)
                    if m:
                        url_sent = True
                        yield f"data: {json.dumps({'url': m.group(0)})}\n\n"
            await proc.wait()
            if proc.returncode == 0:
                yield f"data: {json.dumps({'status': 'ok'})}\n\n"
            else:
                yield f"data: {json.dumps({'status': 'error', 'msg': f'exit {proc.returncode}'})}\n\n"
        except asyncio.TimeoutError:
            yield f"data: {json.dumps({'status': 'error', 'msg': 'timeout'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'status': 'error', 'msg': str(e)})}\n\n"

    return StreamingResponse(generator(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


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
