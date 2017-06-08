package main

import (
	crand "crypto/rand"
	"flag"
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cmu440-F16/paxosapp/rpc/paxosrpc"
	"github.com/cmu440-F16/paxosapp/tests/proxy"
)

type paxosTester struct {
	myPort   int
	numNodes int
	cliMap   map[int]*rpc.Client
	px       proxy.Proxy
}

type testFunc struct {
	name string
	f    func(chan bool)
}

var (
	port       = flag.Int("port", 9019, "port # to listen on")
	numNodes   = flag.Int("N", 1, "number of paxos nodes in the ring")
	paxosPorts = flag.String("paxosports", "", "ports of all nodes")
	nodePxPort = flag.Int("nodeport", 1000, "port of node that is hidden by proxy")
	pxPort     = flag.Int("pxport", 2000, "port that proxy is running on")
	testRegex  = flag.String("t", "", "test to run")
	passCount  int
	failCount  int
	timeout    = 15
	proxy_id   = 0
	pt         *paxosTester
)

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

func initPaxosTester(myPort int, allPorts string, numNodes int, nodeProxyPort int, proxyPort int) (*paxosTester, error) {
	tester := new(paxosTester)
	tester.myPort = myPort
	tester.numNodes = numNodes
	px, err := proxy.NewProxy(nodeProxyPort, proxyPort)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate proxy on port %d", proxyPort)
	}
	tester.px = px

	// Create RPC connection to paxos nodes.
	cliMap := make(map[int]*rpc.Client)
	portList := strings.Split(allPorts, ",")
	for i, p := range portList {
		cli, err := rpc.DialHTTP("tcp", "localhost:"+p)
		if err != nil {
			return nil, fmt.Errorf("could not connect to node %d", i)
		}
		cliMap[i] = cli
	}

	// rpc.RegisterName("LeaseCallbacks", librpc.Wrap(tester))
	// rpc.HandleHTTP()

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", myPort))
	if err != nil {
		LOGE.Fatalln("Failed to listen:", err)
	}
	go http.Serve(l, nil)
	tester.cliMap = cliMap

	// Sleep to allow other nodes to connect to the proxy just started.
	time.Sleep(time.Second * 2)

	return tester, nil
}

func (pt *paxosTester) GetNextProposalNumber(key string, nodeID int) (*paxosrpc.ProposalNumberReply, error) {
	args := &paxosrpc.ProposalNumberArgs{Key: key}
	var reply paxosrpc.ProposalNumberReply
	err := pt.cliMap[nodeID].Call("PaxosNode.GetNextProposalNumber", args, &reply)
	return &reply, err
}

func (pt *paxosTester) GetValue(key string, nodeID int) (*paxosrpc.GetValueReply, error) {
	args := &paxosrpc.GetValueArgs{Key: key}
	var reply paxosrpc.GetValueReply
	err := pt.cliMap[nodeID].Call("PaxosNode.GetValue", args, &reply)
	return &reply, err
}

func (pt *paxosTester) Propose(key string, value interface{}, pnum, nodeID int) (*paxosrpc.ProposeReply, error) {
	args := &paxosrpc.ProposeArgs{N: pnum, Key: key, V: value}
	var reply paxosrpc.ProposeReply
	err := pt.cliMap[nodeID].Call("PaxosNode.Propose", args, &reply)
	return &reply, err
}

func checkProposeReply(reply *paxosrpc.ProposeReply, key string, value uint32) bool {
	if reply.V == nil {
		LOGE.Printf("FAIL: incorrect value from Propose. Got nil, expected %d.\n", value)
		return false
	}

	if value != reply.V {
		LOGE.Printf("FAIL: incorrect value committed. For key %s, got %d, expected %d\n",
			key, reply.V.(uint32), value)
		return false
	}
	return true
}

func checkGetValueAll(key string, value uint32) bool {
	for id, _ := range pt.cliMap {
		r, err := pt.GetValue(key, id)
		if err != nil {
			printFailErr("GetValue", err)
			return false
		}
		if r.V == nil {
			LOGE.Printf("FAIL: Node %d: incorrect value from GetValue on key %s. Got nil, expected %d.\n",
				id, key, value)
			return false
		}
		if r.V.(uint32) != value {
			LOGE.Printf("FAIL: Node %d: incorrect value from GetValue on key %s. Got %d, expected %d.\n",
				id, key, r.V.(uint32), value)
			return false
		}
	}
	return true
}

func runTest(t testFunc, doneChan chan bool) {
	go t.f(doneChan)

	var pass bool
	select {
	case <-time.After(time.Duration(timeout) * time.Second):
		LOGE.Println("FAIL: test timed out")
		pass = false
	case pass = <-doneChan:
		if pt.px.HasError() {
			for _, err := range pt.px.GetError() {
				LOGE.Println(err)
			}
			pass = false
		}
	}
	pt.px.Reset()

	if pass {
		fmt.Println("PASS")
		passCount++
	} else {
		failCount++
	}
}

func printFailErr(fname string, err error) {
	LOGE.Printf("FAIL: error on %s - %s", fname, err)
}

/**
 * Tests that a single proposal should be able to
 * successfully be committed to every node.
 */
func testSingleProposerSingleProposal(doneChan chan bool) {
	key := "a"
	randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
	value := rand.Uint32()
	if _, ok := pt.cliMap[1]; !ok {
		LOGE.Println("FAIL: missing node 1")
		doneChan <- false
		return
	}

	pnum, err := pt.GetNextProposalNumber(key, 1)
	reply, err := pt.Propose(key, value, pnum.N, 1)

	if err != nil {
		printFailErr("Propose", err)
		doneChan <- false
		return
	}

	if !checkProposeReply(reply, key, value) {
		doneChan <- false
		return
	}

	if !checkGetValueAll(key, value) {
		doneChan <- false
		return
	}

	doneChan <- true
}

/**
 * Tests that proposal numbers are monotonically increasing.
 */
func testGetNextProposalNumber(doneChan chan bool) {
	iters := 10
	key := "c"
	randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
	proposer := 1

	usedID := -1

	for i := 0; i < iters; i++ {
		value := rand.Uint32()
		roundIDs := make(map[int]struct{})
		toUse := 0
		for id, _ := range pt.cliMap {
			reply, err := pt.GetNextProposalNumber(key, id)
			if err != nil {
				printFailErr("GetNextProposalNumber", err)
				doneChan <- false
				return
			}
			_, ok := roundIDs[reply.N]
			if ok {
				LOGE.Printf("FAIL: Proposal number %d already used.\n", reply.N)
				doneChan <- false
				return
			}
			roundIDs[reply.N] = struct{}{}

			if id == proposer {
				toUse = reply.N
			}
		}

		if toUse <= usedID {
			LOGE.Printf("FAIL: New proposal number %d less than or equal to number %d already used.\n", toUse, usedID)
			doneChan <- false
			return
		}
		usedID = toUse

		reply, err := pt.Propose(key, value, toUse, proposer)
		if err != nil {
			printFailErr("Propose", err)
			doneChan <- false
			return
		}
		if !checkProposeReply(reply, key, value) {
			doneChan <- false
			return
		}
	}
	doneChan <- true
}

/**
 * Tests that the key is correctly updated in each node's storage system.
 */
func testSingleProposerUpdateKey(doneChan chan bool) {
	key := "b"

	// send first proposal
	randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
	value := rand.Uint32()
	if _, ok := pt.cliMap[1]; !ok {
		LOGE.Println("FAIL: missing node 1")
		doneChan <- false
		return
	}

	pnum, err := pt.GetNextProposalNumber(key, 1)
	reply, err := pt.Propose(key, value, pnum.N, 1)

	if err != nil {
		printFailErr("Propose", err)
		doneChan <- false
		return
	}

	// send second proposal
	randint, _ = crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
	value = rand.Uint32()

	pnum2, err := pt.GetNextProposalNumber(key, 1)
	// the proposal numbers should be monotonically increasing
	if pnum.N >= pnum2.N {
		LOGE.Println("FAIL: newer proposal number is less than or equal to older proposal number")
		doneChan <- false
		return
	}

	reply, err = pt.Propose(key, value, pnum2.N, 1)
	if err != nil {
		printFailErr("Propose", err)
		doneChan <- false
		return
	}

	if !checkProposeReply(reply, key, value) {
		doneChan <- false
		return
	}

	if !checkGetValueAll(key, value) {
		doneChan <- false
		return
	}

	doneChan <- true
}

/**
 * Tests that multiple proposers can send proposals sequentially.
 */
func testMultipleSequentialProposers(doneChan chan bool) {
	for id, _ := range pt.cliMap {
		if id == proxy_id {
			continue
		}

		key := "b" + strconv.Itoa(id)
		randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
		rand.Seed(randint.Int64())
		value := rand.Uint32()
		if _, ok := pt.cliMap[id]; !ok {
			LOGE.Printf("FAIL: missing node %d", id)
			doneChan <- false
			return
		}

		pnum, err := pt.GetNextProposalNumber(key, id)
		reply, err := pt.Propose(key, value, pnum.N, id)

		if err != nil {
			printFailErr("Propose", err)
			doneChan <- false
			return
		}

		if !checkProposeReply(reply, key, value) {
			doneChan <- false
			return
		}

		if !checkGetValueAll(key, value) {
			doneChan <- false
			return
		}
	}
	doneChan <- true
}

func main() {
	btests := []testFunc{
		 {"testSingleProposerSingleProposal", testSingleProposerSingleProposal}, // 4
		 {"testSingleProposerUpdateKey", testSingleProposerUpdateKey}, // 4
		 {"testGetNextProposalNumber", testGetNextProposalNumber}, // 4
		 {"testMultipleSequentialProposers", testMultipleSequentialProposers}, // 4
	}

	flag.Parse()

	// Run the tests with a single tester
	paxosTester, err := initPaxosTester(*port, *paxosPorts, *numNodes, *nodePxPort, *pxPort)
	if err != nil {
		LOGE.Fatalln("Failed to initialize test:", err)
	}
	pt = paxosTester

	doneChan := make(chan bool)
	for _, t := range btests {
		if b, err := regexp.MatchString(*testRegex, t.name); b && err == nil {
			fmt.Printf("Running %s:\n", t.name)
			runTest(t, doneChan)
		}
	}

	fmt.Printf("Passed (%d/%d) tests\n", passCount, passCount+failCount)
}
