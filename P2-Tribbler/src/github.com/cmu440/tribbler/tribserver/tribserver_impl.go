package tribserver

import (
	//	"errors"
	"container/heap"
	"fmt"
	"net"
	http "net/http"
	rpc "net/rpc"
	"strings"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	// TODO: implement this!
	listener net.Listener
	storage  libstore.Libstore
	//	storageCli *rpc.Client
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	ts := new(tribServer)
	var err error
	if ts.listener, err = net.Listen("tcp", myHostPort); err != nil {
		//fmt.Println(myHostPort, err.Error())
		return nil, err
	}
	if err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts)); err != nil {
		return nil, err
	}
	if ts.storage, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal); err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go http.Serve(ts.listener, nil)
	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userid_key := GenUserIDKey(args.UserID)
	//fmt.Println("go here", userid_key)
	var err error
	if _, err = ts.storage.Get(userid_key); err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	//fmt.Println("go here2")
	if err = ts.storage.Put(userid_key, ""); err != nil {
		return err
	}
	//fmt.Println("go here3")
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	var err error
	userid_key := GenUserIDKey(args.UserID)
	if _, err = ts.storage.Get(userid_key); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	target_userid_key := GenUserIDKey(args.TargetUserID)
	if _, err = ts.storage.Get(target_userid_key); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	user_sub_key := GenUserSubscriptionsKey(args.UserID)
	if err = ts.storage.AppendToList(user_sub_key, args.TargetUserID); err != nil {
		if strings.EqualFold(err.Error(), "ItemExists") {
			reply.Status = tribrpc.Exists
			return nil
		}
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	var err error
	userid_key := GenUserIDKey(args.UserID)
	if _, err = ts.storage.Get(userid_key); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	target_userid_key := GenUserIDKey(args.TargetUserID)
	if _, err = ts.storage.Get(target_userid_key); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	user_sub_key := GenUserSubscriptionsKey(args.UserID)
	if err = ts.storage.RemoveFromList(user_sub_key, args.TargetUserID); err != nil {
		if strings.EqualFold(err.Error(), "ItemNotFound") {
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		}
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	var err error
	userid_key := GenUserIDKey(args.UserID)
	if _, err = ts.storage.Get(userid_key); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	user_sub_key := GenUserSubscriptionsKey(args.UserID)
	if reply.UserIDs, err = ts.storage.GetList(user_sub_key); err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	var err error
	userid_key := GenUserIDKey(args.UserID)
	if _, err = ts.storage.Get(userid_key); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	tribble_key_list := GenUserTribblesKey(args.UserID)
	timestamp := time.Now().UnixNano()
	tribble_key := GenTribbleKey(args.UserID, timestamp, args.Contents)
	if err = ts.storage.AppendToList(tribble_key_list, tribble_key); err != nil {
		return err
	}
	if err = ts.storage.Put(tribble_key, args.Contents); err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	var err error
	userid_key := GenUserIDKey(args.UserID)
	if _, err = ts.storage.Get(userid_key); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	var tribble_str_list []string
	tribble_key_list := GenUserTribblesKey(args.UserID)
	if tribble_str_list, err = ts.storage.GetList(tribble_key_list); err != nil {
		return err
	}
	var tribbles []tribrpc.Tribble
	length := len(tribble_str_list)
	for i := length - 1; i >= 0; i-- {
		value := tribble_str_list[i]
		timestamp := GetTribbleTimeFromStr(value)
		var tribble tribrpc.Tribble
		tribble.UserID = args.UserID
		tribble.Posted = time.Unix(0, timestamp).UTC()
		if tribble.Contents, err = ts.storage.Get(value); err != nil {
			continue
		}
		tribbles = append(tribbles, tribble)
		if len(tribbles) >= 100 {
			break
		}
	}
	reply.Tribbles = tribbles
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	var err error
	userid_key := GenUserIDKey(args.UserID)
	if _, err = ts.storage.Get(userid_key); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	user_sub_key := GenUserSubscriptionsKey(args.UserID)
	var sub_list []string
	if sub_list, err = ts.storage.GetList(user_sub_key); err != nil {
		return err
	}
	var tribbles []tribrpc.Tribble
	if tribbles, err = ts.GetLateSubscriptionsTribbes(sub_list); err != nil {
		return err
	}
	reply.Tribbles = tribbles
	reply.Status = tribrpc.OK
	return nil
}

func GenUserIDKey(user string) string {
	return fmt.Sprintf("%s :userID", user)
}

func GenUserSubscriptionsKey(user string) string {
	return fmt.Sprintf("%s :subscriptions_list", user)
}

func GenUserTribblesKey(user string) string {
	return fmt.Sprintf("%s :tribbles_list", user)
}

func GenTribbleKey(user string, timestamp int64, contents string) string {
	return fmt.Sprintf("%s :tribble %v %v", user, timestamp, libstore.StoreHash(contents))
}

func GetTribbleTimeFromStr(tribble_str string) int64 {
	var userID string
	var timestamp int64
	var contentsHash uint64
	fmt.Sscanf(tribble_str, "%s :tribble %v %v", &userID, &timestamp, &contentsHash)
	return timestamp
}

func (ts *tribServer) GetLateSubscriptionsTribbes(sub_list []string) ([]tribrpc.Tribble, error) {
	var err error
	var sub_trib_index []int
	var sub_trib_list [][]string
	var pq priorityQueue
	for pos, sub_userid := range sub_list {
		tribble_key_list := GenUserTribblesKey(sub_userid)
		var tribble_str_list []string
		if tribble_str_list, err = ts.storage.GetList(tribble_key_list); err != nil {
			return nil, err
		}
		sub_trib_list = append(sub_trib_list, tribble_str_list)
		last := len(tribble_str_list) - 1
		sub_trib_index = append(sub_trib_index, last)
		if len(tribble_str_list) == 0 {
			continue
		}
		item := &sortItem{
			pos:       pos,
			key:       tribble_str_list[last],
			timestamp: GetTribbleTimeFromStr(tribble_str_list[last])}
		heap.Push(&pq, item)
	}
	var tribble_keys []string
	for len(tribble_keys) < 100 && pq.Len() > 0 {
		item := heap.Pop(&pq).(*sortItem)
		tribble_keys = append(tribble_keys, item.key)
		sub_trib_index[item.pos]--
		if sub_trib_index[item.pos] >= 0 {
			tribble_str_list := sub_trib_list[item.pos]
			idx := sub_trib_index[item.pos]
			item := &sortItem{
				pos:       item.pos,
				key:       tribble_str_list[idx],
				timestamp: GetTribbleTimeFromStr(tribble_str_list[idx])}
			heap.Push(&pq, item)
		}
	}
	var tribbles []tribrpc.Tribble
	for _, value := range tribble_keys {
		var tribble tribrpc.Tribble
		var timestamp int64
		var contentsHash uint64
		fmt.Sscanf(value, "%s :tribble %v %v", &tribble.UserID, &timestamp, &contentsHash)
		tribble.Posted = time.Unix(0, timestamp).UTC()
		if tribble.Contents, err = ts.storage.Get(value); err != nil {
			return nil, err
		}
		tribbles = append(tribbles, tribble)
	}
	return tribbles, nil
}

type sortItem struct {
	index     int
	pos       int
	key       string
	timestamp int64
}

type priorityQueue []*sortItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].timestamp > pq[j].timestamp
}
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*sortItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
