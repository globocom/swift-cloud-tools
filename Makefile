.PHONY: help setup clean run tests tests_ci deploy_dev

PROJECT_HOME = "`pwd`"

help:
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

setup: ## Install project dependencies
	@pip install -r requirements.txt

clean: ## Clear *.pyc files, etc
	@rm -rf build dist *.egg-info
	@find . \( -name '*.pyc' -o  -name '__pycache__' -o -name '**/*.pyc' -o -name '*~' \) -delete

run: ## Run a development web server
	@PYTHONPATH=`pwd`:$PYTHONPATH python run.py

tests: clean ## Run all tests with coverage
	@echo "Running the tests..."
	@py.test --cov-config .coveragerc --cov $(PROJECT_HOME) --cov-report term-missing

tests_ci: clean ## Run all tests
	@echo "Running the tests..."
	@py.test

deploy_dev: ## Deploy the app to dev
	tsuru app-deploy . -a swift-cloud-tools-dev

deploy_qa: ## Deploy the app to qa
	tsuru app-deploy . -a swift-cloud-tools-qa

deploy_prod: ## Deploy the app to prod
	tsuru app-deploy . -a swift-cloud-tools

deploy_prod_transfer: ## Deploy the app to prod transfer
	tsuru app-deploy . -a swift-cloud-tools-transfer
