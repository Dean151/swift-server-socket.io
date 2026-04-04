test: test-package test-engine

test-package:
	swift test

test-socket:
	docker compose run --build --rm socket-test npm test

down:
	docker compose down --remove-orphans
