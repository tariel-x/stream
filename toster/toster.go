package main

import (
	"errors"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/urfave/cli"
)

func main() {

	app := cli.NewApp()
	app.Name = "Στρεαμ"
	app.Version = "0.1"
	app.Usage = "Στρεαμ"

	app.Commands = []cli.Command{
		{
			Name:    "test",
			Aliases: []string{"t"},
			Usage:   "test cluster",
			Action:  Run,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "nodes, n",
					Usage: "List of nodes separated by comma ','.",
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func Run(c *cli.Context) error {
	nodesListString := c.String("nodes")
	if nodesListString == "" {
		return errors.New("invalid nodes list")
	}
	nodes := strings.Split(nodesListString, ",")

	wg := &sync.WaitGroup{}
	for _, node := range nodes {
		toster := newToster(node)
		go toster.Begin(wg)
	}
	wg.Wait()
	return nil
}

type Toster struct {
	node string
}

func newToster(node string) *Toster {
	return &Toster{node: node}
}

func (t *Toster) Begin(wg *sync.WaitGroup) {
	defer wg.Done()
	return
}
