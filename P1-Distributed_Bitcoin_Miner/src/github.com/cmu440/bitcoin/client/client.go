package main

import (
	"encoding/json"
	"fmt"
  "strconv"
	"os"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}
	upper, err := strconv.ParseUint(os.Args[3], 10, 32)
	if err != nil {
		return
	}

	// TODO: implement this!
	c, err := lsp.NewClient(os.Args[1], lsp.NewParams())
	if err != nil {
		fmt.Println(err.Error())
		printDisconnected()
		return
	}
	req := bitcoin.NewRequest(os.Args[2], 0, upper)
	payload, err := json.Marshal(req)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
  err = c.Write(payload)
	if err != nil {
		fmt.Println(err.Error())
		printDisconnected()
		return
	}
	payload, err = c.Read()
	if err != nil {
		fmt.Println(err.Error())
		printDisconnected()
		return
	}
	var msg bitcoin.Message
	err = json.Unmarshal(payload, &msg)
	if err != nil {
    return
	}
  hash := strconv.FormatUint(msg.Hash, 10)
  nonce := strconv.FormatUint(msg.Nonce, 10)
  printResult(hash, nonce)
}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
