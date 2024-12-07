FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy simulation scripts
COPY . .

# Make scripts executable
RUN chmod +x minio_generator.py minio_watcher.py

# Default command
CMD ["python", "minio_watcher.py", \
     "--endpoint", "${MINIO_ENDPOINT}", \
     "--access-key", "${MINIO_ACCESS_KEY}", \
     "--secret-key", "${MINIO_SECRET_KEY}", \
     "--api-url", "${API_URL}", \
     "--patterns", ".csv"]
