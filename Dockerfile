FROM python:3.12.13-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/signalbot-venv \
    UV_LINK_MODE=copy \
    PATH="/opt/signalbot-venv/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN pip install --no-cache-dir uv==0.11.22 \
    && uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY config ./config
RUN uv sync --frozen --no-dev --no-editable
RUN useradd --create-home --uid 10001 signalbot && mkdir -p /app/var && chown -R signalbot:signalbot /app
USER signalbot
CMD ["signalbot", "run", "--config", "config/settings.example.yaml"]
