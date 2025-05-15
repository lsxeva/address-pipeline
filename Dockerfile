# Dockerfile

FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY requirements.txt .
COPY src/ src/
COPY main.py .
COPY test_performance.py .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV PYTHONPATH=/app

# Run the application
CMD ["python", "main.py"]
