# Contributing 🌳

## Prerequisites

- make
- python >= 3.10
- uv >= 0.5.0

## Getting started

`make install` creates the dev environment with:

- a virtualenv in _.venv/_
- git hooks for formatting & linting on git push (these also run in CI)

`. .venv/bin/activate` activates the virtualenv.

The make targets will update the virtualenv when _pyproject.toml_ changes.

## Usage

Run `make` to see the options for running tests, linting, formatting etc.

## Raising a PR

PR titles use [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) prefixes where:

- `feat` adding an unimplemented feature
- `fix` fixing an already implemented feature

Changes to behaviour covered by a test is considered a breaking change. Breaking changes are indicated with an exclamation mark in the title.

New features and bug fixes require a minimal test case that mimics the behaviour of Snowflake and passes if run against a real Snowflake instance, or documents clearly where it deviates.
