"""Bridge HTTP — expose claude CLI (~/.local/bin/claude) via HTTP pour l'addon Docker."""
import asyncio
import logging
import os
import re
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", os.path.expanduser("~/.local/bin/claude"))
TIMEOUT_S = 90

app = FastAPI(title="Claude Bridge")


class AskRequest(BaseModel):
    system: str
    prompt: str


class AskResponse(BaseModel):
    response: str
    tts: str


@app.get("/health")
async def health():
    return {"status": "ok", "claude": CLAUDE_BIN}


@app.post("/ask", response_model=AskResponse)
async def ask(req: AskRequest):
    if not os.path.isfile(CLAUDE_BIN):
        raise HTTPException(500, f"claude introuvable : {CLAUDE_BIN}")

    log.info("Prompt reçu (%d chars)", len(req.prompt))

    try:
        proc = await asyncio.create_subprocess_exec(
            CLAUDE_BIN,
            "--system", req.system,
            "--allowedTools", "bash",
            "-p", req.prompt,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=TIMEOUT_S)
    except asyncio.TimeoutError:
        proc.kill()
        log.error("claude timeout (%ds)", TIMEOUT_S)
        raise HTTPException(504, "claude timeout")
    except Exception as e:
        log.error("claude exec error: %s", e)
        raise HTTPException(500, str(e))

    if proc.returncode != 0:
        err = stderr.decode()[:300]
        log.error("claude exit %d: %s", proc.returncode, err)
        raise HTTPException(500, f"claude exit {proc.returncode}: {err}")

    output = stdout.decode()
    log.info("Réponse claude (%d chars)", len(output))

    match = re.search(r"RÉPONSE_VOCALE\s*:\s*(.+)", output, re.IGNORECASE)
    tts = match.group(1).strip() if match else ""

    if not tts:
        lines = [l.strip() for l in output.splitlines() if l.strip()]
        tts = lines[-1] if lines else ""

    return AskResponse(response=output, tts=tts)


if __name__ == "__main__":
    port = int(os.environ.get("BRIDGE_PORT", "9099"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
