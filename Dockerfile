# ==============================================================================
# Dockerfile for UrbanPulse Cassandra Streamlit Dashboard
# Base Image: Python 3.11-slim (optimized size and full compatibility)
# ==============================================================================

FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files and dataset
COPY app.py .
COPY queries.py .
COPY db_setup.py .
COPY generate_dataset.py .
COPY air_quality_data.csv .

# Expose standard Streamlit port
EXPOSE 8501

# Healthcheck to verify the web service is responding
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Launch the Streamlit dashboard
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
