package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/urfave/cli"

	storage "github.com/tariel-x/whynot/log"
	"github.com/tariel-x/whynot/paxos"
	"github.com/tariel-x/whynot/server"
	"github.com/tariel-x/whynot/stream"
)

var backgroundContext context.Context

func main() {
	app := cli.NewApp()
	app.Name = "Στρεαμ"
	app.Version = "0.1"
	app.Usage = "Στρεαμ is distributed log made with Paxos"

	app.Commands = []cli.Command{
		{
			Name:    "run",
			Aliases: []string{"r"},
			Usage:   "run node",
			Action:  Run,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "nodes, n",
					Usage: "List of nodes separated by comma ','. Current node would be skipped.",
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
	nodes := strings.Split(nodesListString, ",")

	pxs, err := paxos.NewWnPaxos(nodes)
	if err != nil {
		return err
	}

	lg, err := storage.NewLog()
	if err != nil {
		return err
	}

	hndlr, err := stream.NewHandler(lg, pxs)
	if err != nil {
		return err
	}

	srv, err := server.NewServer(listenAddress, hndlr)
	if err != nil {
		return err
	}
	return srv.Run(backgroundContext)
}
