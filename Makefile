build:
	docker compose up build
	docker build -t ls:1 .

test:
	docker compose up example