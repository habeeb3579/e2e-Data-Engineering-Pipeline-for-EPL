FROM python:3.9-slim

WORKDIR /app

# Install system dependencies including Chrome and chromedriver for Selenium
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    unzip \
    gnupg \
    curl \
    && curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d '.' -f 1) \
    && CHROMEDRIVER_VERSION=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION") \
    && wget -q "https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip" \
    && unzip chromedriver_linux64.zip -d /usr/local/bin/ \
    && rm chromedriver_linux64.zip \
    && chmod +x /usr/local/bin/chromedriver

# Install Python dependencies
COPY requirements-scraper.txt /tmp/requirements-scraper.txt
RUN pip install --no-cache-dir -r /tmp/requirements-scraper.txt

# Copy source code
COPY src /app/src
COPY config /app/config

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONPATH=/app

# Default command
CMD ["python", "-m", "src.extraction.understat.scraper"]