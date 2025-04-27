.PHONY: help setup install clean test lint format docker-build docker-up docker-down scrape process load transform orchestrate dashboard terraform-init terraform-plan terraform-apply docs

# Variables
PYTHON = python3
PIP = $(PYTHON) -m pip
PYTEST = pytest
BLACK = black
ISORT = isort
FLAKE8 = flake8
MYPY = mypy
DOCKER_COMPOSE = docker-compose
TERRAFORM = terraform

# Default target
help:
	@echo "Football Data Pipeline - Makefile Help"
	@echo "========================================"
	@echo "setup              - Create virtual environment and install dependencies"
	@echo "install            - Install project dependencies"
	@echo "clean              - Remove build artifacts and cache directories"
	@echo "test               - Run tests with pytest"
	@echo "lint               - Run linters (flake8, mypy)"
	@echo "format             - Format code with black and isort"
	@echo "docker-build       - Build all Docker images"
	@echo "docker-up          - Start all services with Docker Compose"
	@echo "docker-down        - Stop all services"
	@echo "scrape             - Run the data scraper"
	@echo "process            - Run the data processor"
	@echo "load               - Load data into data warehouse"
	@echo "transform          - Run dbt transformations"
	@echo "orchestrate        - Start the orchestration services"
	@echo "dashboard          - Start the Streamlit dashboard"
	@echo "terraform-init     - Initialize Terraform"
	@echo "terraform-plan     - Create Terraform execution plan"
	@echo "terraform-apply    - Apply Terraform changes"
	@echo "docs               - Generate documentation"

# Setup environment
setup:
	$(PYTHON) -m venv venv
	@echo "Activate virtual environment with: source venv/bin/activate"
	$(MAKE) install

# Install dependencies
install:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt

# Clean project
clean:
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Run tests
test:
	$(PYTEST) tests/ --cov=src --cov-report=term-missing

# Lint code
lint:
	$(FLAKE8) src/ tests/
	$(MYPY) src/

# Format code
format:
	$(BLACK) src/ tests/
	$(ISORT) src/ tests/

# Docker commands
docker-build:
	$(DOCKER_COMPOSE) build

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

# Data pipeline commands
scrape:
	$(PYTHON) -m src.extraction.understat.scraper

process:
	$(PYTHON) -m src.processing.spark_processor

load:
	$(PYTHON) -m src.extraction.understat.loader

transform:
	cd dbt && dbt run

orchestrate:
	$(DOCKER_COMPOSE) up -d airflow-webserver airflow-scheduler kestra

dashboard:
	$(DOCKER_COMPOSE) up -d streamlit

# Terraform commands
terraform-init:
	cd infrastructure/gcp && $(TERRAFORM) init

terraform-plan:
	cd infrastructure/gcp && $(TERRAFORM) plan -var-file=terraform.tfvars

terraform-apply:
	cd infrastructure/gcp && $(TERRAFORM) apply -var-file=terraform.tfvars

# Generate documentation
docs:
	sphinx-build -b html docs/source docs/build