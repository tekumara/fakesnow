MAKEFLAGS += --warn-undefined-variables
SHELL = /bin/bash -o pipefail
.DEFAULT_GOAL := help
.PHONY: help .uv .sync clean install check format pyright test dist hooks install-hooks

## display help message
help:
	@awk '/^##.*$$/,/^[~\/\.0-9a-zA-Z_-]+:/' $(MAKEFILE_LIST) | awk '!(NR%2){print $$0p}{p=$$0}' | awk 'BEGIN {FS = ":.*?##"}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' | sort

## check that uv is installed
.uv:
	@uv --version || { echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/' && exit 13 ;}

.sync:
	uv sync --extra server $(if $(value CI),,--group notebook)

# delete the venv
clean:
	rm -rf uv.lock .venv

## create venv and install this package and hooks
install: .uv .sync $(if $(value CI),,install-hooks)

## format, lint and type check
check: export SKIP=test
check: hooks

## format and lint
format: export SKIP=pyright,test
format: hooks

## pyright type check
pyright:
	PYRIGHT_PYTHON_IGNORE_WARNINGS=1 uv run pyright

## run tests
test:
	uv run pytest

## build python distribution
dist:
# start with a clean slate (see setuptools/#2347)
	rm -rf build dist *.egg-info
	uv run -m build --wheel

## publish to pypi
publish:
	uv run twine upload dist/*

## run pre-commit git hooks on all files
hooks:
	uv run pre-commit run --color=always --all-files --hook-stage push

install-hooks: .git/hooks/pre-push

.git/hooks/pre-push:
	uv run pre-commit install --install-hooks -t pre-push
