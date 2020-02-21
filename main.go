package main

import (
	"context"
	"errors"
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
	if listenAddress == "" {
		return errors.New("invalid listen address")
	}

	nodesListString := c.String("nodes")
	if nodesListString == "" {
		return errors.New("invalid nodes list")
	}
	allNodes := strings.Split(nodesListString, ",")
	nodes := make([]string, 0, len(allNodes)-1)
	for _, node := range allNodes {
		if node != listenAddress {
			nodes = append(nodes, node)
		}
	}

	pxs, err := paxos.NewPaxos(nodes, listenAddress)
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
