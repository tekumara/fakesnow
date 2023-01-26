# Contributing 🌳

## Prerequisites

- make
- node (required for pyright)
- python >= 3.9

## Getting started

`make install` creates the dev environment with:

- a virtualenv in _.venv/_
- pyright in _node_modules/_
- git hooks for formatting & linting on git push

`. .venv/bin/activate` activates the virtualenv.

The make targets will update the virtualenv when _pyproject.toml_ changes.

## Usage

Run `make` to see the options for running tests, linting, formatting etc.
