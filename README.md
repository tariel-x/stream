# Στρεαμ

Στρεαμ is the simple distributed stream-processing system.

Key ~~features~~:

- :bomb: No persistence;
- :boom: No topics;
- :fire: No acknowledges.

## Build

Requirements: go 1.12+

`go build -o stream_server`

## Run

`./stream_server run --listen=localhost:7001 --nodes=localhost:7001,localhost:7002,localhost:7003`

Where:

- `listen` - host to listen;
- `nodes` - other stream nodes.

## Usage

### Go client library

Download library.

`go get github.com/tariel-x/stream/client`

Example usage:

```go
package main

import (
	"log"

	"github.com/tariel-x/stream/client"
)

func main() {
	c, _ := client.New("localhost:7001", nil)
	conn, _ := c.Connect()
	defer conn.Close()
	responses, _ := conn.QueryMany(&client.Pull{N: 0})
	for {
		err := responses.Err()
		if err != nil {
			log.Println(err)
			break
		}
		response := responses.Next()
		if response == nil {
			break
		}
		log.Print(response.Message)
	}
}

```

### Client protocol

1. `PUSH a` - push value `a` to the cluster;
2. `PULL 0` - start reading log from the epoch `0`. NB! epoch is not a value number in the values list.
3. `GET 0` - read log from the epoch `o` to the end of the values list.

## Internal

Στρεαμ implements [Paxos](https://www.microsoft.com/en-us/research/uploads/prod/2016/12/The-Part-Time-Parliament.pdf) consensus protocol.