MAKEFLAGS += --warn-undefined-variables
SHELL = /bin/bash -o pipefail
.DEFAULT_GOAL := help
.PHONY: help clean install format check pyright test dist hooks install-hooks

## display help message
help:
	@awk '/^##.*$$/,/^[~\/\.0-9a-zA-Z_-]+:/' $(MAKEFILE_LIST) | awk '!(NR%2){print $$0p}{p=$$0}' | awk 'BEGIN {FS = ":.*?##"}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' | sort

venv ?= .venv
pip := $(venv)/bin/pip

$(pip): $(if $(value CI),|,) .python-version
# create venv using system python even when another venv is active
	PATH=$${PATH#$${VIRTUAL_ENV}/bin:} python3 -m venv --clear $(venv)
	$(venv)/bin/python --version
	$(pip) install pip~=23.3 wheel~=0.40

$(venv): $(if $(value CI),|,) pyproject.toml $(pip)
	$(pip) install -e '.[dev, notebook]'
	touch $(venv)

node_modules: package.json
	npm install --no-save
	touch node_modules

# delete the venv
clean:
	rm -rf $(venv)

## create venv and install this package and hooks
install: $(venv) node_modules $(if $(value CI),,install-hooks)

## format, lint and type check
check: export SKIP=test
check: hooks

## format and lint
format: export SKIP=pyright,test
format: hooks

## pyright type check
pyright: node_modules $(venv)
	node_modules/.bin/pyright

## run tests
test: $(venv)
	$(venv)/bin/pytest

## build python distribution
dist: $(venv)
	rm -rf build dist *.egg-info
	$(venv)/bin/python -m build --sdist --wheel

## publish to pypi
publish: $(venv)
	$(venv)/bin/twine upload dist/*

## run pre-commit git hooks on all files
hooks: node_modules $(venv)
	$(venv)/bin/pre-commit run --color=always --all-files --hook-stage push

install-hooks: .git/hooks/pre-push

.git/hooks/pre-push: $(venv)
	$(venv)/bin/pre-commit install --install-hooks -t pre-push
