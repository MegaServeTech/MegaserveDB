# Use Python 3.11 slim for a lightweight base image
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies required for MySQL + cryptography
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libssl-dev \
    libffi-dev \
    python3-dev \
    gcc \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

EXPOSE 8000

# Use flask for development (you can change this to gunicorn or waitress later for prod)
CMD ["flask", "run", "--host=0.0.0.0", "--port=8000", "--reload"]
