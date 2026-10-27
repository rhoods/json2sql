.PHONY: check fmt-check lint test

check: fmt-check lint test

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy -- -D warnings -D clippy::too_many_lines -D clippy::cognitive_complexity

test:
	cargo test
