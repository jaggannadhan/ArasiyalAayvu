.PHONY: run-fe run-be

DEFAULT_BACKEND_TASK := awareness
BACKEND_PYTHON ?= .venv/bin/python
BACKEND_TASK ?=
BACKEND_TASK_FROM_GOAL := $(word 2,$(MAKECMDGOALS))
RESOLVED_BACKEND_TASK := $(if $(BACKEND_TASK),$(BACKEND_TASK),$(if $(BACKEND_TASK_FROM_GOAL),$(BACKEND_TASK_FROM_GOAL),$(DEFAULT_BACKEND_TASK)))

run-fe:
	cd web && npm run dev:hot

run-be:
	.venv/bin/uvicorn web.backend_api.main:app --reload --port 8000

%:
	@:
