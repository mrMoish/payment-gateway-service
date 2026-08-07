FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# The database lives on a volume so operations, intents, providerPaymentId and
# event history survive `docker compose down` + `up`.
ENV DATABASE_PATH=/data/payments.db
RUN mkdir -p /data && useradd --create-home --uid 10001 service && chown -R service /data /srv
USER service

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2).status == 200 else 1)"

# One worker process: the submission worker runs in-process, and a single writer
# keeps SQLite contention low. Horizontal scaling would need a shared database,
# but the lease + immutable Idempotency-Key design already tolerates it.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--no-access-log"]
