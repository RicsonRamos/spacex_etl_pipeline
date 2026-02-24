# Makefile para facilitar execução de tarefas comuns

.PHONY: test run lint

# Testes com pytest
test:
	python -m pytest --maxfail=5 --disable-warnings -v

# Executa o pipeline
run:
	python src/etl/etl_flow.py

# Linting com ruff
lint:
	ruff check .
