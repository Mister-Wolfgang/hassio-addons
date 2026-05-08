ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
    ffmpeg \
    ffmpeg-dev \
    gcc \
    musl-dev \
    pkgconf \
    libffi-dev \
    openssl-dev \
    cython \
    nodejs \
    npm \
    gcompat \
    libstdc++ \
    libgcc

RUN npm install -g @anthropic-ai/claude-code

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir cython && \
    pip install --no-cache-dir -r requirements.txt && \
    python -c "from openwakeword.model import Model; Model(wakeword_models=['hey_jarvis']); print('OWW model hey_jarvis downloaded')"

COPY . .
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]

LABEL \
    io.hass.version="1.0.6" \
    io.hass.type="addon" \
    io.hass.arch="aarch64|amd64"
