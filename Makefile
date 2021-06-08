.PHONY: help setup clean run tests

# Version package
VERSION=$(shell python -c 'import swift_cloud_tools; print swift_cloud_tools.VERSION')

PROJECT_HOME = "`pwd`"

procs = $(shell ps -ef | grep run_test | grep -v grep | awk '{ print $$2; }')
killcmd = $(if $(procs), "kill" "-9" $(procs), "echo" "no matching processes")

help:
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

setup: ## Install project dependencies
	@pip install -r requirements.txt

clean: ## Clear *.pyc files, etc
	@rm -rf build dist *.egg-info
	@find . \( -name '*.pyc' -o  -name '__pycache__' -o -name '**/*.pyc' -o -name '*~' \) -delete

run: ## Run a development web server
	@PYTHONPATH=`pwd`:$PYTHONPATH python run.py

run_expirer: ## Run expirer
	@PYTHONPATH=`pwd`:$PYTHONPATH python swift_cloud_tools/server/expirer.py

run_tests: ## Run a development web server
	@PYTHONPATH=`pwd`:$PYTHONPATH python run_test.py &
	@sleep 3

tests: clean run_tests ## Run all tests with coverage
	@echo "Running the tests..."
	@py.test --cov-config .coveragerc --cov $(PROJECT_HOME) --cov-report term-missing
	@echo 'processes == ['$(procs)']'
	@$(killcmd)

tests_ci: clean run_tests ## Run all tests
	@echo "Running the tests..."
	@py.test
	@echo 'processes == ['$(procs)']'
	@$(killcmd)

deploy_dev: ## Deploy the app to dev
	tsuru app-deploy . -a swift-cloud-tools-dev
