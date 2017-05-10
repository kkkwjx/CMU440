package libstore

import (
  "fmt"
	"container/list"
	"errors"
	rpc "net/rpc"
	"time"
	//http "net/http"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

const (
	MAXN = 1024
)

type libstore struct {
	// TODO: implement this!
	masterCli               *rpc.Client
	slavesCli               map[uint32]*rpc.Client
	hostPort                string
	mode                    LeaseMode
	serverAddrs             []string
	tw                      timeWheel
	kvCache                 map[string]interface{}
	cacheTime               map[string]int
	cacheOrNot, replyRevoke chan bool
	queryCh, revokeChan     chan string
	addCh                   chan cacheInfo
	answerCh                chan interface{}
	ticker                  *time.Ticker
}

type cacheInfo struct {
	key     string
	value   interface{}
	seconds int
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	ls := &libstore{
		ticker:   time.NewTicker(time.Millisecond * 1000),
		hostPort: myHostPort,
		mode:     mode,
    slavesCli: make(map[uint32]*rpc.Client, 1),
    kvCache:  make(map[string]interface{}, 1),
    cacheTime:make(map[string]int, 1),
    cacheOrNot:  make(chan bool, MAXN),
    replyRevoke:  make(chan bool, MAXN),
    queryCh:  make(chan string, MAXN),
    revokeChan:make(chan string, MAXN),
    addCh:    make(chan cacheInfo, MAXN),
    answerCh: make(chan interface{}, MAXN)}
  if len(myHostPort) == 0 {
    ls.mode = Never
  }
	var err error
	if err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls)); err != nil {
		return nil, err
	}
	if ls.masterCli, err = rpc.DialHTTP("tcp", masterServerHostPort); err != nil {
		return nil, err
	}
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	for i := 0; i < 5; i++ {
		if err := ls.masterCli.Call("StorageServer.GetServers", args, &reply); err == nil {
			if reply.Status == storagerpc.OK {
        index := uint32(0)
				for _, serv := range reply.Servers {
					slaveCli, err := rpc.DialHTTP("tcp", serv.HostPort)
					if err != nil {
            fmt.Println(err.Error())
						return nil, err
					}
					ls.slavesCli[index] = slaveCli
          index++
				}
				break
			}
		}
		time.Sleep(time.Millisecond * 1000)
	}
  if reply.Status != storagerpc.OK {
    return nil, errors.New("Error")
  }
	ls.tw.init(storagerpc.QueryCacheSeconds)
	go ls.runInLoop()
	go ls.tw.runInLoop()
	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
  ls.queryCh <- key
	exist := <-ls.cacheOrNot
	if exist {
		value := <-ls.answerCh
		str_value, ok := value.(string)
    if ok {
		  return str_value, nil
    } else {
      return "", errors.New("type error.")
    }
	}
	ls.tw.queryCh <- key
	cnt := <-ls.tw.answerCh
	wantLease := false
	if cnt >= storagerpc.QueryCacheThresh && ls.mode == Normal || ls.mode == Always {
		wantLease = true
	}
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostPort}
	var reply storagerpc.GetReply
	cli := ls.RoutingServer(key)
	if err := cli.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}
	if reply.Status != storagerpc.OK {
		return "", errors.New("Error Get")
	}
	if reply.Lease.Granted {
		ci := cacheInfo{
			key:     key,
			value:   reply.Value,
			seconds: reply.Lease.ValidSeconds}
		ls.addCh <- ci
	}
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	var reply storagerpc.PutReply
  args := &storagerpc.PutArgs{
                   Key: key,
                   Value: value}
	cli := ls.RoutingServer(key)
	if err := cli.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}
  if reply.Status != storagerpc.OK {
    return errors.New("Error Put")
  }
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.queryCh <- key
	exist := <-ls.cacheOrNot
	if exist {
		value := <-ls.answerCh
		splice_value, ok := value.([]string)
    if ok {
		  return splice_value, nil
    } else {
      return nil, errors.New("type error.")
    }
	}
	ls.tw.queryCh <- key
	cnt := <-ls.tw.answerCh
	wantLease := false
	if cnt >= storagerpc.QueryCacheThresh && ls.mode == Normal || ls.mode == Always {
		wantLease = true
	}
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostPort}
	var reply storagerpc.GetListReply
	cli := ls.RoutingServer(key)
	if err := cli.Call("StorageServer.GetList", args, &reply); err != nil {
		return nil, err
	}
  if reply.Status == storagerpc.KeyNotFound {
    return nil, nil
  }
	if reply.Status != storagerpc.OK {
		return nil, errors.New("Error GetList")
	}
	if reply.Lease.Granted {
		ci := cacheInfo{
			key:     key,
			value:   reply.Value,
			seconds: reply.Lease.ValidSeconds}
		ls.addCh <- ci
	}
	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	var reply storagerpc.PutReply
  args := &storagerpc.PutArgs{
                   Key: key,
                   Value: removeItem}
	cli := ls.RoutingServer(key)
	if err := cli.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}
  if reply.Status == storagerpc.ItemNotFound {
    return errors.New("ItemNotFound")
  }
  if reply.Status != storagerpc.OK {
    return errors.New("Error RemoveFromList")
  }
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	var reply storagerpc.PutReply
  args := &storagerpc.PutArgs{
                   Key: key,
                   Value: newItem}
	cli := ls.RoutingServer(key)
	if err := cli.Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}
  if reply.Status == storagerpc.ItemExists {
    return errors.New("ItemExists")
  }
  if reply.Status != storagerpc.OK {
    return errors.New("Error AppendToList")
  }
	return nil
}

func (ls *libstore) RoutingServer(key string) *rpc.Client {
	hash := StoreHash(key)
  index := hash % uint32(len(ls.slavesCli))
	return ls.slavesCli[index]
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.revokeChan <- args.Key
	exist := <-ls.replyRevoke
	if !exist {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ls *libstore) runInLoop() {
	for {
		select {
		case <-ls.ticker.C:
			for key, t := range ls.cacheTime {
				if t == 0 {
					delete(ls.cacheTime, key)
					delete(ls.kvCache, key)
				} else {
					ls.cacheTime[key] = ls.cacheTime[key] - 1
				}
			}
		case key := <-ls.revokeChan:
			if _, ok := ls.cacheTime[key]; ok {
				delete(ls.cacheTime, key)
				delete(ls.kvCache, key)
				ls.replyRevoke <- true
			} else {
				ls.replyRevoke <- false
			}
		case key := <-ls.queryCh:
			if value, ok := ls.kvCache[key]; ok {
				ls.answerCh <- value
				ls.cacheOrNot <- true
				break
			}
			ls.cacheOrNot <- false
		case ci := <-ls.addCh:
			ls.cacheTime[ci.key] = ci.seconds
			ls.kvCache[ci.key] = ci.value
		}
	}
}

type timeWheel struct {
	circle   *list.List
	ticker   *time.Ticker
	count    map[string]int
	queryCh  chan string
	answerCh chan int
}

func (tw *timeWheel) init(size int) {
	tw.circle = list.New()
	for i := 0; i < size; i++ {
		tw.circle.PushBack(make(map[string]int, 1))
	}
	tw.ticker = time.NewTicker(time.Millisecond * 1000)
	tw.count = make(map[string]int, 1)
	tw.queryCh = make(chan string, MAXN)
	tw.answerCh = make(chan int, MAXN)
}

func (tw *timeWheel) runInLoop() {
	tail := make(map[string]int, 1)
	for {
		select {
		case <-tw.ticker.C:
			e := tw.circle.Front()
			head := e.Value.(map[string]int)
			for k, v := range head {
				tw.count[k] = tw.count[k] - v
			}
			tw.circle.Remove(e)
			tw.circle.PushBack(tail)
			for k, v := range tail {
				tw.count[k] = tw.count[k] + v
			}
			for k, v := range tw.count {
				if v == 0 {
					delete(tw.count, k)
				}
			}
			tail = make(map[string]int, 1)
		case key := <-tw.queryCh:
			ans := 0
			if _, ok := tail[key]; ok {
				//ans = ans + cnt
				tail[key] = tail[key] + 1
			} else {
				tail[key] = 1
			}
			if cnt, ok := tail[key]; ok {
				ans = ans + cnt
			}
			tw.answerCh <- ans
		}
	}
}
