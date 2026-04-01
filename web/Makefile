.PHONY: run-fe run-be install-be-deps

BACKEND_PYTHON ?= ../.venv/bin/python
BACKEND_HOST ?= 0.0.0.0
BACKEND_PORT ?= 8000

run-fe:
	npm run dev:hot

install-be-deps:
	@if [ -x "$(BACKEND_PYTHON)" ]; then \
		$(BACKEND_PYTHON) -m pip install -r backend_api/requirements.txt; \
	else \
		python3 -m pip install -r backend_api/requirements.txt; \
	fi

run-be: install-be-deps
	@if [ -x "$(BACKEND_PYTHON)" ]; then \
		$(BACKEND_PYTHON) -m uvicorn backend_api.main:app --reload --host $(BACKEND_HOST) --port $(BACKEND_PORT); \
	else \
		python3 -m uvicorn backend_api.main:app --reload --host $(BACKEND_HOST) --port $(BACKEND_PORT); \
	fi
