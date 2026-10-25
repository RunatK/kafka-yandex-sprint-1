.PHONY: test test-docker up lint lint-fix

test:
	uv run --locked python -m unittest discover -s tests -v

test-docker:
	docker build -f docker/Dockerfile-tests -t kafka-sprint-1-tests .
	docker run --rm kafka-sprint-1-tests

up:
	docker compose --env-file .env.example --env-file .env --profile workers up -d --build

lint:
	uv run --locked ruff check .

lint-fix:
	uv run --locked ruff check . --fix
