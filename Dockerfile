FROM python:3.11-slim-bookworm

# Tizim paketlari (psycopg2 va boto3 uchun)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Requirements avval nusxalanadi (Docker layer cache uchun)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Barcha fayllar
COPY . .

# Uploads papkasi (local fallback uchun)
RUN mkdir -p uploads/images uploads/avatars

# Port
ENV PORT=8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${PORT}/ping || exit 1

# Ishga tushirish
CMD ["sh", "-c", "uvicorn server:app --host 0.0.0.0 --port ${PORT} --workers 1"]