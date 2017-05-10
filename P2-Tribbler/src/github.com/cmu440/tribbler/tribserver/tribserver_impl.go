package tribserver

import (
	"errors"
  "fmt"
  "time"
	"net"
	rpc "net/rpc"
  http "net/http"

	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/libstore"
)

type tribServer struct {
	// TODO: implement this!
	listener       net.Listener
  storage        libstore.Libstore
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
    fmt.Println(myHostPort, err.Error())
		return nil, err
	}
  if ts.storage, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal); err != nil {
    return nil, err
  }
	if err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts)); err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go http.Serve(ts.listener, nil)
	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
  userid_key := GenUserIDKey(args.UserID)
  var err error
  if _, err = ts.storage.Get(userid_key); err == nil {
    reply.Status = tribrpc.Exists
	  return errors.New("UserID is existed.")
  }
  if err = ts.storage.Put(userid_key, ""); err != nil {
    return err
  }
  reply.Status = tribrpc.OK
  return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
  var err error
  userid_key := GenUserIDKey(args.UserID)
  if _, err = ts.storage.Get(userid_key); err != nil {
    reply.Status = tribrpc.NoSuchUser
	  return errors.New("UserID is not existed.")
  }
  target_userid_key := GenUserIDKey(args.TargetUserID)
  if _, err = ts.storage.Get(target_userid_key); err != nil {
    reply.Status = tribrpc.NoSuchTargetUser
	  return errors.New("TargetUserID is not existed.")
  }
  user_sub_key := GenUserSubscriptionsKey(args.UserID)
  if err = ts.storage.AppendToList(user_sub_key, args.TargetUserID); err != nil {
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
	  return errors.New("UserID is not existed.")
  }
  target_userid_key := GenUserIDKey(args.TargetUserID)
  if _, err = ts.storage.Get(target_userid_key); err != nil {
    reply.Status = tribrpc.NoSuchTargetUser
	  return errors.New("TargetUserID is not existed.")
  }
  user_sub_key := GenUserSubscriptionsKey(args.UserID)
  if err = ts.storage.RemoveFromList(user_sub_key, args.TargetUserID); err != nil {
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
	  return errors.New("UserID is not existed.")
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
	  return errors.New("UserID is not existed.")
  }
  tribble_key_list := GenUserTribblesKey(args.UserID)
  timestamp := time.Now().Unix()
  tribble_key := GenTribbleKey(args.UserID, timestamp, args.Contents)
  if err = ts.storage.AppendToList(tribble_key, tribble_key_list); err != nil {
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
	  return errors.New("UserID is not existed.")
  }
  if reply.Tribbles, err = ts.GetTribblesByUserID(args.UserID); err != nil {
    return err
  }
  reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
  var err error
  userid_key := GenUserIDKey(args.UserID)
  if _, err = ts.storage.Get(userid_key); err != nil {
    reply.Status = tribrpc.NoSuchUser
	  return errors.New("UserID is not existed.")
  }
  user_sub_key := GenUserSubscriptionsKey(args.UserID)
  var sub_list []string
  if sub_list, err = ts.storage.GetList(user_sub_key); err != nil {
    return err
  }
  for _, sub_userid := range sub_list {
    var tribbles []tribrpc.Tribble
    if tribbles, err = ts.GetTribblesByUserID(sub_userid); err != nil {
      continue
    }
    for _, tribble := range tribbles {
      reply.Tribbles = append(reply.Tribbles, tribble)
    }
  }
  reply.Status = tribrpc.OK
	return nil
}

func GenUserIDKey(user string) string {
  return fmt.Sprintf("%s:user", user)
}

func GenUserSubscriptionsKey(user string) string {
  return fmt.Sprintf("%s:subscriptions_list", user)
}

func GenUserTribblesKey(user string) string {
  return fmt.Sprintf("%s:tribbles_list", user)
}

func GenTribbleKey(user string, timestamp int64, contents string) string {
  return fmt.Sprintf("%s:tribble %d %u", user, timestamp, libstore.StoreHash(contents))
}

func GetTribbleTimeFromStr(tribble_str string) int64 {
  var userID string
  var timestamp int64
  var contentsHash uint64
  fmt.Sscanf(tribble_str, "%s:tribble %d %u", userID, timestamp, contentsHash)
  return timestamp
}

func (ts * tribServer) GetTribblesByUserID(userID string) ([]tribrpc.Tribble, error) {
  var err error
  var tribble_str_list []string
  tribble_key_list := GenUserTribblesKey(userID)
  if tribble_str_list, err = ts.storage.GetList(tribble_key_list); err != nil {
    return nil, err
  }
  var tribbles []tribrpc.Tribble
  for _, value := range tribble_str_list {
    timestamp := GetTribbleTimeFromStr(value)
    var tribble tribrpc.Tribble
    tribble.UserID = userID
    tribble.Posted = time.Unix(timestamp, 0)
    if tribble.Contents, err = ts.storage.Get(value); err != nil {
      continue
    }
    tribbles = append(tribbles, tribble)
  }
  return tribbles, nil
}
