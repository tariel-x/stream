# Στρεαμ

Στρεαμ is the simple distributed stream-processing system.

Key ~~features~~:

- :bomb: No persistence;
- :boom: No topics;
- :fire: No acknowledges.

## Build

`go build -o stream_server`

## Run

`./stream_server run --listen=localhost:7001 --nodes=localhost:7001,localhost:7002,localhost:7003`

Where:

- `listen` - host to listen;
- `nodes` - other stream nodes.
