package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/urfave/cli"
)

var backgroundContext context.Context

func main() {
	app := cli.NewApp()
	app.Name = "WhyNpt"
	app.Version = "0.1"
	app.Usage = "WhyNot is distributed log made with Paxos"

	app.Commands = []cli.Command{
		{
			Name:    "run",
			Aliases: []string{"r"},
			Usage:   "run node",
			Action:  Run,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "nodes, n",
					Usage: "List of nodes including current",
				},
				cli.StringFlag{
					Name:  "listen, l",
					Usage: "Listen interface:port",
				},
			},
		},
	}

	// listen signals
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	defer close(sigs)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-ctx.Done():
		case <-sigs:
			log.Println("terminating")
			cancel()
		}
	}()
	backgroundContext = ctx

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func Run(c *cli.Context) error {
	listenAddress := c.String("listen")

	nodesListString := c.String("nodes")
	nodesList := strings.Split(nodesListString, ",")

	server, err := NewServer(listenAddress, nodesList)
	if err != nil {
		return err
	}
	return server.Run(backgroundContext)
}
