prog := plsync

debug ?=

$(info debug is $(debug))

ifdef debug
  release :=
  target :=debug
  extension :=debug
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
	cp target/$(target)/$(prog) ~/bin/$(prog)-$(extension)

example:
	cargo run --example disk_usage $(release) .

all: build test install
 
help:
	@echo "usage: make $(prog) [debug=1]"
	@echo "       make example [debug=1]"
