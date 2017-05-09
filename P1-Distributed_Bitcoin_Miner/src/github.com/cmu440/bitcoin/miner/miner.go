package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}

	// TODO: implement this!
	c, err := lsp.NewClient(os.Args[1], lsp.NewParams())
	if err != nil {
		fmt.Println(err.Error())
		return
	}
  join := bitcoin.NewJoin()
  payload, _ := json.Marshal(join)
  err = c.Write(payload)
  if err != nil {
    fmt.Println(err.Error())
    return
  }
	ch := make(chan *bitcoin.Message, 1)
	go func(c lsp.Client, ch chan *bitcoin.Message) {
		for {
			payload, err := c.Read()
			var msg bitcoin.Message
			err = json.Unmarshal(payload, &msg)
			if err != nil {
				continue
			}
			ch <- &msg
		}
	}(c, ch)
	for {
		select {
		case msg := <-ch:
			if msg.Type == bitcoin.Request {
				go func(c lsp.Client, msg *bitcoin.Message) {
					bestNonce := msg.Lower
					bestHash := bitcoin.Hash(msg.Data, msg.Lower)
					for i := msg.Lower + 1; i <= msg.Upper; i++ {
						hash := bitcoin.Hash(msg.Data, i)
						if hash < bestHash {
							bestHash = hash
							bestNonce = i
						}
					}
					msg.Type = bitcoin.Result
					msg.Hash = bestHash
					msg.Nonce = bestNonce
					payload, _ := json.Marshal(msg)
					err := c.Write(payload)
					if err != nil {
						fmt.Println(err.Error())
					}
				}(c, msg)
			}
		}
	}
}
