FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements-streamlit.txt /tmp/requirements-streamlit.txt
RUN pip install --no-cache-dir -r /tmp/requirements-streamlit.txt

# Copy streamlit app and source code
COPY streamlit /app
COPY src /app/src
COPY config /app/config

# Set environment variables
ENV PYTHONPATH=/app

# Run streamlit
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]