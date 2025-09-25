.PHONY: build up down sh logs upload clean full-update

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

sh:
	docker compose exec app bash

logs:
	docker compose logs -f --tail=200

upload:
	docker compose run --rm upload

clean:
	docker compose down -v --remove-orphans
	docker system prune -f

full-update:
	docker compose run --rm full_update
