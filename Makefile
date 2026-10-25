.PHONY: test test-docker infra-up infra-wait topics-init workers-up up lint lint-fix

test:
	uv run --locked python -m unittest discover -s tests -v

test-docker:
	docker build -f docker/Dockerfile-tests -t kafka-sprint-1-tests .
	docker run --rm kafka-sprint-1-tests

infra-up:
	docker compose --env-file .env.example --env-file .env up -d zookeeper kafka1 kafka2 schema-registry kafka-ui

infra-wait:
	COMPOSE_ENV_FILES=.env.example,.env bash scripts/kafka_wait.sh

topics-init:
	COMPOSE_ENV_FILES=.env.example,.env bash scripts/kafka_init.sh

workers-up:
	docker compose --env-file .env.example --env-file .env --profile workers up -d --build --no-deps produce-order-worker single-consume-order-worker batch-consume-order-worker

up:
	$(MAKE) infra-up
	$(MAKE) infra-wait
	$(MAKE) topics-init
	$(MAKE) workers-up

lint:
	uv run --locked ruff check .

lint-fix:
	uv run --locked ruff check . --fix
