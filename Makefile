# ArboVigilancia SP - Makefile
# EC2 Production Commands

.PHONY: help docker-build docker-up docker-down docker-logs aws-check test clean

help:
	@echo "================================================"
	@echo "  ArboVigilancia SP - Commands"
	@echo "================================================"
	@echo "  make docker-build       - Build Streamlit container"
	@echo "  make docker-up          - Start Streamlit dashboard"
	@echo "  make docker-down        - Stop dashboard"
	@echo "  make docker-logs        - View service logs"
	@echo "  make aws-check          - Verify AWS credentials/IAM"
	@echo "  make test               - Run unit tests"
	@echo "  make clean              - Clean cache and temp files"
	@echo "================================================"

docker-build:
	docker-compose -f docker/docker-compose.yml build

docker-up:
	docker-compose -f docker/docker-compose.yml up -d

docker-down:
	docker-compose -f docker/docker-compose.yml down

docker-logs:
	docker-compose -f docker/docker-compose.yml logs -f

aws-check:
	aws sts get-caller-identity

test:
	pytest tests/ -v

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
