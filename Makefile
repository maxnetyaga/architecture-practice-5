.PHONY: build, c-run, i-test, unit-test

build:
	docker-compose -f docker-compose.yaml -f docker-compose.test.yaml \
		build

i-test:
	docker-compose -f docker-compose.yaml -f docker-compose.test.yaml \
		up

c-run:
	docker-compose up

unit-test:
	go test -v ./cmd/balancer/...