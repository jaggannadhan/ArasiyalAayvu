.PHONY: run-fe run-be

BACKEND_TASK ?= awareness
BACKEND_PYTHON ?= ../.venv/bin/python

run-fe:
	npm run dev:hot

run-be:
	@if [ -x "$(BACKEND_PYTHON)" ]; then \
		$(BACKEND_PYTHON) ../main.py --task $(BACKEND_TASK); \
	else \
		python3 ../main.py --task $(BACKEND_TASK); \
	fi
