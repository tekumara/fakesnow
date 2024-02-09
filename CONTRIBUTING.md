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

## Raising a PR

PR titles use [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) prefixes where:

- `feat` adding an unimplemented feature
- `fix` fixing an already implemented feature

And breaking changes are indicated with an exclamation mark in the title.
