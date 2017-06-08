// DO NOT MODIFY!

package main


import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/cmu440-F16/paxosapp/paxos"
	"strconv"
)

var (
	ports      = flag.String("ports", "", "ports for all paxos nodes")
	myport	   = flag.Int("myport", 9990, "port this paxos node should listen to")
	numNodes   = flag.Int("N", 1, "the number of nodes in the ring")
	nodeID     = flag.Int("id", 0, "node ID must match index of this node's port in the ports list")
	numRetries = flag.Int("retries", 5, "number of times a node should retry dialing another node")
	replace    = flag.Bool("replace", false, "if this node is a replacement node")
	proxyPort  = flag.Int("pxport", 9999, "(Staff use) port that the proxy server listens on")
	proxyNode  = flag.Int("proxy", -1, "(Staff use) node being monitored by the proxy")
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	flag.Parse()

	portStrings := strings.Split(*ports, ",")

	hostMap := make(map[int]string)
	if *proxyNode >= 0 {
		for i, port := range portStrings {
			if i == *proxyNode {
				hostMap[*proxyNode] = fmt.Sprintf("localhost:%d", *proxyPort)
			} else {
				hostMap[i] = "localhost:" + port
			}
		}
	} else {
		for i, port := range portStrings {
			hostMap[i] = "localhost:" + port
		}
	}

	// Create and start the Paxos Node.
	_, err := paxos.NewPaxosNode("localhost:" + strconv.Itoa(*myport), hostMap, *numNodes, *nodeID, *numRetries, *replace)
	if err != nil {
		log.Fatalln("Failed to create paxos node:", err)
	}

	// Run the paxos node forever.
	select {}
}
