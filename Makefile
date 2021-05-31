.PHONY: help setup clean run tests

# Version package
VERSION=$(shell python -c 'import swift_cloud_tools; print swift_cloud_tools.VERSION')

PROJECT_HOME = "`pwd`"

help:
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

setup: ## Install project dependencies
	@pip install -r requirements.txt

clean: ## Clear *.pyc files, etc
	@rm -rf build dist *.egg-info
	@find . \( -name '*.pyc' -o  -name '__pycache__' -o -name '**/*.pyc' -o -name '*~' \) -delete

run: ## Run a development web server
	@PYTHONPATH=`pwd`:$PYTHONPATH python3.6 run.py

tests: clean ## Run all tests with coverage
	@py.test --cov-config .coveragerc --cov $(PROJECT_HOME) --cov-report term-missing