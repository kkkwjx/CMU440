package storageserver

import (
	//	"errors"
	"fmt"
	"log"
	"net"
	http "net/http"
	rpc "net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

var (
	logger *log.Logger
)

type storageServer struct {
	// TODO: implement this!
	masterCli                           *rpc.Client
	clientMutex                         sync.Mutex
	clients                             map[string]*rpc.Client
	listener                            net.Listener
	numNodes                            int
	nodeID                              uint32
	sortedServerId                      libstore.UintArray
	servers                             []storagerpc.Node
	storeMutex, leaseMutex, keyMapMutex sync.Mutex
	keyMutex                            map[string]*sync.Mutex
	kvStore                             map[string]interface{}
	leaseTime                           map[string]map[string]int
	connServers                         map[uint32]bool
	doneJoin                            chan bool
	ticker                              *time.Ticker
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	var err error
	ss := &storageServer{
		ticker:         time.NewTicker(time.Millisecond * 1000),
		sortedServerId: libstore.UintArray{},
		keyMutex:       make(map[string]*sync.Mutex),
		clients:        make(map[string]*rpc.Client, 1),
		leaseTime:      make(map[string]map[string]int, 1),
		connServers:    make(map[uint32]bool, 1),
		kvStore:        make(map[string]interface{}, 1),
		doneJoin:       make(chan bool, 1),
		numNodes:       numNodes,
		nodeID:         nodeID}
	serverLogFile, err := os.OpenFile("log_storage.txt", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err.Error())
	}
	logger = log.New(serverLogFile, "Storage-- ", log.Lmicroseconds|log.Lshortfile)
	hostIp := fmt.Sprintf(":%d", port)
	if ss.listener, err = net.Listen("tcp", hostIp); err != nil {
		return nil, err
	}
	if err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss)); err != nil {
		return nil, err
	}
	if len(masterServerHostPort) == 0 {
		serverInfo := storagerpc.Node{NodeID: nodeID,
			HostPort: fmt.Sprintf("localhost:%d", port)}
		ss.servers = append(ss.servers, serverInfo)
		ss.connServers[nodeID] = true
	}
	rpc.HandleHTTP()
	go http.Serve(ss.listener, nil)
	go ss.runInLoop()
	if len(masterServerHostPort) != 0 {
		if ss.masterCli, err = rpc.DialHTTP("tcp", masterServerHostPort); err != nil {
			return nil, err
		}
		var reply storagerpc.RegisterReply
		args := &storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{NodeID: nodeID,
				HostPort: fmt.Sprintf("localhost:%d", port)}}
		for {
			if err = ss.masterCli.Call("StorageServer.RegisterServer", args, &reply); err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.OK {
				ss.servers = reply.Servers
				return ss, nil
			}
			time.Sleep(time.Millisecond * 1000)
		}
	} else {
		if numNodes != 1 {
			for {
				select {
				case <-ss.doneJoin:
					return ss, nil
				}
			}
		} else {
			return ss, nil
		}
	}
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if _, ok := ss.connServers[args.ServerInfo.NodeID]; !ok {
		ss.servers = append(ss.servers, args.ServerInfo)
		ss.connServers[args.ServerInfo.NodeID] = true
		logger.Println("Join", args.ServerInfo.NodeID)
    if len(ss.connServers) == ss.numNodes {
		  ss.doneJoin <- true
    }
	}
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	right := ss.checkKeyRangeOrNot(args.Key)
	if !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	mutex := ss.GetKeyMutex(args.Key)
	mutex.Lock()
	defer mutex.Unlock()
	e, ok := ss.kvStore[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value, ok = e.(string)
		if !ok {
			return nil
		}
		if args.WantLease {
			ss.setLeaseTime(args.Key, args.HostPort, storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)
		}
		reply.Lease = storagerpc.Lease{
			Granted:      args.WantLease,
			ValidSeconds: storagerpc.LeaseSeconds}
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	right := ss.checkKeyRangeOrNot(args.Key)
	if !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	mutex := ss.GetKeyMutex(args.Key)
	mutex.Lock()
	defer mutex.Unlock()
	e, ok := ss.kvStore[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = e.([]string)
		if args.WantLease {
			ss.setLeaseTime(args.Key, args.HostPort, storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)
		}
		reply.Lease = storagerpc.Lease{
			Granted:      args.WantLease,
			ValidSeconds: storagerpc.LeaseSeconds}
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	right := ss.checkKeyRangeOrNot(args.Key)
	if !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	mutex := ss.GetKeyMutex(args.Key)
	mutex.Lock()
	defer mutex.Unlock()
	ss.waitAllKeyLeaseExpired(args.Key)
	ss.storeMutex.Lock()
	defer ss.storeMutex.Unlock()
	ss.kvStore[args.Key] = args.Value
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	right := ss.checkKeyRangeOrNot(args.Key)
	if !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	mutex := ss.GetKeyMutex(args.Key)
	mutex.Lock()
	defer mutex.Unlock()
	ss.waitAllKeyLeaseExpired(args.Key)
	ss.storeMutex.Lock()
	defer ss.storeMutex.Unlock()
	e, ok := ss.kvStore[args.Key]
	var list []string
	if ok {
		list = e.([]string)
	}
	for _, value := range list {
		if strings.EqualFold(value, args.Value) {
			reply.Status = storagerpc.ItemExists
			return nil
		}
	}
	list = append(list, args.Value)
	ss.kvStore[args.Key] = list
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	right := ss.checkKeyRangeOrNot(args.Key)
	if !right {
		reply.Status = storagerpc.WrongServer
		logger.Println("wrongserver remove list", args.Key)
		return nil
	}
	mutex := ss.GetKeyMutex(args.Key)
	mutex.Lock()
	defer mutex.Unlock()
	ss.waitAllKeyLeaseExpired(args.Key)
	ss.storeMutex.Lock()
	defer ss.storeMutex.Unlock()
	e, ok := ss.kvStore[args.Key]
	if ok {
		list := e.([]string)
		for index, value := range list {
			if strings.EqualFold(value, args.Value) {
				if len(list) == 1 {
					delete(ss.kvStore, args.Key)
				} else {
					list = append(list[:index], list[index+1:]...)
					ss.kvStore[args.Key] = list
				}
				reply.Status = storagerpc.OK
				return nil
			}
		}
	}
	reply.Status = storagerpc.ItemNotFound
	return nil
}

func (ss *storageServer) GetKeyMutex(key string) *sync.Mutex {
	ss.keyMapMutex.Lock()
	defer ss.keyMapMutex.Unlock()
	if _, ok := ss.keyMutex[key]; !ok {
		ss.keyMutex[key] = new(sync.Mutex)
	}
	return ss.keyMutex[key]
}

func (ss *storageServer) checkKeyRangeOrNot(key string) bool {
	if len(ss.sortedServerId) == 0 {
		for _, value := range ss.servers {
			ss.sortedServerId = append(ss.sortedServerId, value.NodeID)
		}
		sort.Sort(ss.sortedServerId)
		for _, value := range ss.sortedServerId {
			logger.Println("sorted id :", value)
		}
	}
	partition_keys := strings.Split(key, ":")
	hash := libstore.StoreHash(partition_keys[0])
	for _, value := range ss.sortedServerId {
		logger.Println(value, hash, ss.nodeID)
		if value >= hash {
			logger.Println(value, hash, ss.nodeID)
			return value == ss.nodeID
		}
	}
	return ss.nodeID == ss.sortedServerId[0]
}

func (ss *storageServer) setLeaseTime(key, addr string, sec int) {
	defer ss.leaseMutex.Unlock()
	ss.leaseMutex.Lock()
	if _, ok := ss.leaseTime[key]; !ok {
		ss.leaseTime[key] = make(map[string]int, 1)
	}
	ss.leaseTime[key][addr] = sec
}

func (ss *storageServer) queryLeaseTime(key, addr string) int {
	defer ss.leaseMutex.Unlock()
	ss.leaseMutex.Lock()
	if _, ok := ss.leaseTime[key]; !ok {
		return 0
	}
	if _, ok := ss.leaseTime[key][addr]; !ok {
		return 0
	}
	return ss.leaseTime[key][addr]
}

func (ss *storageServer) getNoExpiredKeyAddr(key string) []string {
	var addrs []string
	defer ss.leaseMutex.Unlock()
	ss.leaseMutex.Lock()
	if value, ok := ss.leaseTime[key]; ok {
		for addr, sec := range value {
			if sec > 0 {
				addrs = append(addrs, addr)
			}
		}
	}
	return addrs
}

func (ss *storageServer) waitOneKeyLeaseExpired(key, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	done := make(chan bool, 1)
	go func(ss *storageServer, key, addr string, done chan bool) {
		ss.clientMutex.Lock()
		cli, ok := ss.clients[addr]
		if !ok {
			var err error
			if ss.clients[addr], err = rpc.DialHTTP("tcp", addr); err != nil {
				ss.clientMutex.Unlock()
				return
			}
			cli = ss.clients[addr]
		}
		ss.clientMutex.Unlock()

		args := &storagerpc.RevokeLeaseArgs{Key: key}
		var reply storagerpc.RevokeLeaseReply
		if err := cli.Call("LeaseCallbacks.RevokeLease", args, &reply); err != nil {
			return
		}
		if reply.Status == storagerpc.OK {
			done <- true
		}
	}(ss, key, addr, done)
	ticker := time.NewTicker(time.Millisecond * 1000)
	for {
		select {
		case <-ticker.C:
			if ss.queryLeaseTime(key, addr) == 0 {
				return
			}
		case <-done:
			ss.setLeaseTime(key, addr, 0)
			return
		}
	}
}

func (ss *storageServer) waitAllKeyLeaseExpired(key string) {
	addrs := ss.getNoExpiredKeyAddr(key)
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Add(1)
		go ss.waitOneKeyLeaseExpired(key, addr, &wg)
	}
	wg.Wait()
}

func (ss *storageServer) runInLoop() {
	for {
		select {
		case <-ss.ticker.C:
			ss.leaseMutex.Lock()
			for key, _ := range ss.leaseTime {
				for addr, _ := range ss.leaseTime[key] {
					ss.leaseTime[key][addr]--
					if ss.leaseTime[key][addr] <= 0 {
						delete(ss.leaseTime[key], addr)
					}
				}
				if len(ss.leaseTime[key]) == 0 {
					delete(ss.leaseTime, key)
				}
			}
			ss.leaseMutex.Unlock()
		}
	}
}
