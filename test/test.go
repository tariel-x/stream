package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"

	"github.com/urfave/cli"

	"github.com/tariel-x/stream/client"
)

func main() {

	app := cli.NewApp()
	app.Name = "Στρεαμ test"
	app.Version = "0.1"
	app.Usage = "Στρεαμ test"

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
	nodeAddresses := strings.Split(nodesListString, ",")
	tosts := []*Toster{}
	for _, node := range nodeAddresses {
		toster, err := newToster(node)
		if err != nil {
			return err
		}
		tosts = append(tosts, toster)
	}

	wg := &sync.WaitGroup{}
	for _, tost := range tosts {
		wg.Add(1)
		go tost.Begin(wg)
	}
	wg.Wait()

	wg = &sync.WaitGroup{}
	for _, tost := range tosts {
		wg.Add(1)
		go tost.Read(wg)
	}
	wg.Wait()

	for _, tost := range tosts {
		log.Println(tost.results)
	}
	return nil
}

type Toster struct {
	node    string
	client  *client.Client
	prefix  string
	results []string
}

func newToster(node string) (*Toster, error) {
	client, err := client.New(node, nil)
	prefix := string(byte(97 + rand.Intn(26)))
	return &Toster{
		node:    node,
		client:  client,
		prefix:  prefix,
		results: []string{},
	}, err
}

func (t *Toster) Begin(wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("%s%d", t.prefix, i)
		log.Println("send PUSH", msg, "to", t.node)
		response, err := t.client.QueryOne(&client.Push{V: msg})
		if err != nil {
			log.Println("error", err)
			continue
		}
		ok, err := response.Ok()
		if err != nil {
			log.Println("error", err)
			continue
		}
		if !ok {
			log.Printf("PUSH %s failed", msg)
		}
	}
}

func (t *Toster) Read(wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("GET 0 from %s", t.node)
	responses, err := t.client.QueryMany(&client.Get{N: 0})
	if err != nil {
		log.Println("error", err)
	}
	t.results = []string{}
	for _, response := range responses {
		t.results = append(t.results, response.Message)
	}
}
