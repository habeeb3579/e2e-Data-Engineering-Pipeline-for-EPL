FROM python:3.9-slim

WORKDIR /dbt

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install DBT with PostgreSQL adapter
RUN pip install --no-cache-dir dbt-core==1.5.2 dbt-postgres==1.5.2

# Copy dbt project files
COPY dbt /dbt

# Set working directory to dbt project
WORKDIR /dbt

# Default command
ENTRYPOINT ["dbt"]
CMD ["--help"]