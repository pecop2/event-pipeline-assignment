
.PHONY: build run test unit-test integration-test run-mysql down lint clean

# Run the application locally
run: 
	docker-compose up -d --build

# Run all tests (unit + integration)
test: unit-test integration-test

# Run unit tests only
unit-test:
	go test ./tests/unit/... -v

# Run integration tests (starts MySQL first)
integration-test: run-mysql
	sleep 15 # wait for MySQL to initialize
	go test ./tests/integration/... -v

# Start Docker Compose (MySQL + app dependencies)
run-mysql:
	docker-compose up -d mysql mysql-init

# Stop Docker Compose services
down:
	docker-compose down
