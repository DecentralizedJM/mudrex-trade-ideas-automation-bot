FROM python:3.11-slim

WORKDIR /app

# Install git for SDK installation from GitHub
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# Copy source code
COPY src/ src/

# Create data directory for SQLite (will be mounted as volume in Railway)
RUN mkdir -p /app/data

# Default database path (override with Railway volume)
ENV DATABASE_PATH=/app/data/subscribers.db

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the server
CMD ["python", "-m", "signal_bot.run"]
