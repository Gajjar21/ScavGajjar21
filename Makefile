PYTHON ?= python3

.PHONY: setup setup-dev run config lint test check precommit

setup:
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

setup-dev:
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt -r requirements-dev.txt

run:
	$(PYTHON) -m V3.app

config:
	$(PYTHON) -m V3.config

lint:
	$(PYTHON) -m ruff check V3

test:
	$(PYTHON) -m pytest

check:
	$(PYTHON) -m ruff check V3
	$(PYTHON) -m compileall -q V3
	$(PYTHON) -m pytest

precommit:
	$(PYTHON) -m pre_commit run --all-files
