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
    npm

RUN npm install -g @anthropic-ai/claude-code

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir cython && \
    pip install --no-cache-dir -r requirements.txt

COPY . .
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]

LABEL \
    io.hass.version="1.0.3" \
    io.hass.type="addon" \
    io.hass.arch="aarch64|amd64"
