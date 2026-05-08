ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    nodejs \
    npm \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g @anthropic-ai/claude-code

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -c "from openwakeword.model import Model; Model(wakeword_models=['hey_jarvis']); print('OWW model hey_jarvis downloaded')"

COPY . .
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]

LABEL \
    io.hass.version="1.0.7" \
    io.hass.type="addon" \
    io.hass.arch="aarch64|amd64"
