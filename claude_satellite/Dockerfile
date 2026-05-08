ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
    ffmpeg \
    gcc \
    musl-dev \
    nodejs \
    npm \
    gcompat \
    libstdc++ \
    libgcc

RUN npm install -g @anthropic-ai/claude-code

WORKDIR /app

# onnxruntime n'a pas de wheel musllinux — on télécharge le wheel manylinux
# et on l'installe manuellement (fonctionne grâce à gcompat sur Alpine)
RUN pip download onnxruntime \
        --platform manylinux_2_17_x86_64 \
        --python-version 312 \
        --implementation cp \
        --abi cp312 \
        --only-binary :all: \
        --index-url https://pypi.org/simple \
        -d /tmp/oww_wheels \
    && pip install --no-deps /tmp/oww_wheels/onnxruntime-*.whl \
    && rm -rf /tmp/oww_wheels

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -c "from openwakeword.model import Model; Model(wakeword_models=['hey_jarvis']); print('OWW model hey_jarvis downloaded')"

COPY . .
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]

LABEL \
    io.hass.version="1.0.9" \
    io.hass.type="addon" \
    io.hass.arch="aarch64|amd64"
