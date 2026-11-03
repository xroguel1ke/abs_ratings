FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    CONFIG_DIR=/config PORT=47823 TZ=Europe/Berlin

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && apt-get update && apt-get install -y --no-install-recommends tzdata curl && rm -rf /var/lib/apt/lists/*

COPY abs_ratings.py abs_series.py abs_undo.py recorder.py ./
COPY webui ./webui

ARG VERSION=dev
ENV APP_VERSION=$VERSION
VOLUME ["/config"]
EXPOSE 47823
HEALTHCHECK --interval=2m --timeout=10s --start-period=30s CMD curl -fsS "http://127.0.0.1:${PORT}/health" || exit 1
CMD ["python", "-m", "webui.app"]
