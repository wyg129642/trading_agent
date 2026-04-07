FROM python:3.11-slim AS base

WORKDIR /app

# System deps for psycopg2, lxml, playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

# Install Playwright browsers
RUN playwright install chromium --with-deps

# Copy source
COPY backend/ /app/backend/
COPY engine/ /app/engine/
COPY config/ /app/config/

# Alembic config
COPY backend/alembic.ini /app/alembic.ini

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
