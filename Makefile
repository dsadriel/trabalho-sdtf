PYTHON ?= python3
PORT ?= 4000
ARGS ?=

.PHONY: all build test server client

all: build

build:
	$(PYTHON) -m compileall -q cliente.py servidor.py client common server

test:
	$(PYTHON) -B -m unittest discover -s tests -v

server:
	$(PYTHON) servidor.py $(PORT) $(ARGS)

client:
	$(PYTHON) cliente.py $(PORT) $(ARGS)
