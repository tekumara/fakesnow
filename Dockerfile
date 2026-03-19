# Multi-stage Dockerfile for fakesnow
ARG VERSION=dev
ARG GIT_COMMIT=unknown

FROM python:3.11-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy only necessary files for dependency installation
COPY pyproject.toml README.md LICENSE ./
COPY fakesnow/ ./fakesnow/

# Install dependencies in a virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install the application with server dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir .[server]

# Production stage
FROM python:3.11-slim AS production

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create non-root user
RUN groupadd -r fakesnow && useradd -r -g fakesnow -d /app -s /bin/bash fakesnow

# Set work directory
WORKDIR /app

# Copy application code
COPY --chown=fakesnow:fakesnow fakesnow/ ./fakesnow/
COPY --chown=fakesnow:fakesnow pyproject.toml ./

# The application is already installed in the venv, no need to reinstall

# Create directories for database persistence
RUN mkdir -p /app/databases /app/logs && \
    chown -R fakesnow:fakesnow /app

# Copy entrypoint script
COPY --chown=fakesnow:fakesnow docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Switch to non-root user
USER fakesnow

# OCI image labels
ARG VERSION
ARG GIT_COMMIT
LABEL org.opencontainers.image.title="fakesnow" \
      org.opencontainers.image.description="Fake Snowflake Connector for Python. Run, mock and test Snowflake DB locally." \
      org.opencontainers.image.source="https://github.com/tekumara/fakesnow" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}"

# Expose port (default 8080, configurable via environment)
EXPOSE 8080

# Environment variables
ENV FAKESNOW_PORT=8080
ENV FAKESNOW_HOST=0.0.0.0
ENV FAKESNOW_DB_PATH=/app/databases
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check - expect 405 for GET /session (proves server is working)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${FAKESNOW_PORT}/session || [ $? -eq 22 ]


# Set entrypoint
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# Default command - run the server
CMD ["server"]
