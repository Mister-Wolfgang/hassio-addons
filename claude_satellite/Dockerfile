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
    libgcc \
    libgomp

RUN npm install -g @anthropic-ai/claude-code

WORKDIR /app

# onnxruntime n'a pas de wheel musllinux — téléchargement du wheel manylinux
# + extraction directe dans site-packages (bypass vérif plateforme, gcompat requis)
RUN pip download onnxruntime \
        --platform manylinux_2_27_x86_64 \
        --python-version 312 \
        --implementation cp \
        --abi cp312 \
        --only-binary :all: \
        --no-deps \
        --index-url https://pypi.org/simple \
        -d /tmp/oww_wheels \
    && python -c "import zipfile,site,glob; sp=site.getsitepackages()[0]; [zipfile.ZipFile(w).extractall(sp) or print('Extracted',w) for w in glob.glob('/tmp/oww_wheels/onnxruntime-*.whl')]" \
    && rm -rf /tmp/oww_wheels

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -c "from openwakeword.model import Model; Model(wakeword_models=['hey_jarvis']); print('OWW model hey_jarvis downloaded')"

COPY . .
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]

LABEL \
    io.hass.version="1.0.12" \
    io.hass.type="addon" \
    io.hass.arch="aarch64|amd64"
