prog := plsync

debug ?=

$(info debug is $(debug))

ifdef debug
  release :=
  target :=debug
  extension :=-debug
else
  release :=--release
  target :=release
  extension :=
endif

build:
	cargo build $(release)

test:
	cargo test

install:
	cp target/$(target)/$(prog) ~/bin/$(prog)$(extension)

lint:
	cargo clippy --all-targets -- -D warnings

publish:
	cargo publish

upgrade:
	cargo upgrade -i allow
	cargo update

all: build test install

docker:
	docker build -t plsync .

.PHONY: all build test install docker help

help:
	@echo "usage: make $(prog) [debug=1]"
