.PHONY: check lint test

check: lint test

lint:
	cargo clippy -- -D warnings -D clippy::too_many_lines -D clippy::cognitive_complexity

test:
	cargo test
