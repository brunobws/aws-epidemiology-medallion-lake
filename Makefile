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

# ===== EC2 DEPLOYMENT =====
.PHONY: ec2-setup ec2-deploy ec2-restart ec2-stop

ec2-setup:
	@echo "Installing Docker dependencies..."
	sudo apt update -y && sudo apt install -y git docker.io docker-compose
	sudo usermod -aG docker $$USER
	sudo systemctl start docker
	sudo systemctl enable docker
	@echo "✓ Docker setup complete. Run: newgrp docker"

ec2-deploy:
	@echo "Pulling latest code..."
	git pull origin main
	@echo "Building Docker image..."
	make docker-build
	@echo "Stopping old containers..."
	make docker-down
	@echo "Starting dashboard..."
	make docker-up
	@echo "✓ Dashboard running at http://$(EC2_IP):8501"

ec2-restart:
	make docker-down
	make docker-up
	make docker-logs

ec2-stop:
	make docker-down
	@echo "✓ Dashboard stopped"

ec2-status:
	docker ps --filter "name=arbovigilancia"
