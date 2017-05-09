package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"strconv"
)

const MAXN = 1024

var (
	logger *log.Logger
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}

	serverLogFile, err := os.OpenFile("server_log.txt", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err.Error())
	}
	logger = log.New(serverLogFile, "Server-- ", log.Lmicroseconds|log.Lshortfile)
	// TODO: implement this!
	port, err := strconv.Atoi(os.Args[1])
	s, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		logger.Println(err.Error())
		return
	}
	defer s.Close()
	iMsgCh := make(chan *internalMsg, 1)
	go func(s lsp.Server, iMsgCh chan *internalMsg) {
		for {
			connID, payload, err := s.Read()
			iMsg := &internalMsg{
				connID:  connID,
				isClose: false,
				data:    payload}
			if err != nil {
				logger.Println(err.Error())
				iMsg.isClose = true
			}
			logger.Printf("recv %d\n", connID)
			iMsgCh <- iMsg
		}
	}(s, iMsgCh)
	cp := &clientPool{clients: make(map[int]*client, 1)}
	mp := &minerPool{
		taskList:    list.New(),
		availMiners: make(chan *miner, MAXN),
		miners:      make(map[int]*miner, 1)}
	for {
		select {
		case iMsg := <-iMsgCh:
			if iMsg.isClose {
				// client down
				if _, ok := cp.clients[iMsg.connID]; ok {
					logger.Printf("client %d quit.\n", iMsg.connID)
					delete(cp.clients, iMsg.connID)
					s.CloseConn(iMsg.connID)
				} else {
					// miner down
					if m, ok := mp.miners[iMsg.connID]; ok {
						task := m.sendIngTask
						delete(mp.miners, iMsg.connID)
						if _, ok = cp.clients[task.connID]; ok {
							mp.taskList.PushBack(task)
						}
						logger.Printf("miner %d quit.\n", iMsg.connID)
					}
				}
			} else {
				var msg bitcoin.Message
				err := json.Unmarshal(iMsg.data, &msg)
				if err != nil {
					logger.Println(err.Error())
					continue
				}
				logger.Println(msg.String())
				if msg.Type == bitcoin.Request {
					taskList := mp.genTasks(iMsg.connID, msg.Data, msg.Lower, msg.Upper)
					cp.clients[iMsg.connID] = &client{
						minHash:        ^uint64(0),
						assignMinerCnt: taskList.Len(),
						doneMinerCnt:   0}
					logger.Printf("client %d join.\n", iMsg.connID)
					logger.Printf("tasks len %d.\n", taskList.Len())
					mp.taskList.PushBackList(taskList)
				} else if msg.Type == bitcoin.Result {
					if m, ok := mp.miners[iMsg.connID]; ok {
						task := m.sendIngTask
						m.sendIngTask = nil
						mp.availMiners <- m
						if c, ok := cp.clients[task.connID]; ok {
							c.doneMinerCnt++
							if c.minHash > msg.Hash {
								c.minHash = msg.Hash
								c.nonce = msg.Nonce
							}
							if c.doneMinerCnt == c.assignMinerCnt {
								res := bitcoin.NewResult(c.minHash, c.nonce)
								err := sendMessage(s, res, task.connID)
								if err != nil {
									break
								}
								delete(cp.clients, task.connID)
								s.CloseConn(task.connID)
							}
						}
					}
				} else if msg.Type == bitcoin.Join {
					m := &miner{connID: iMsg.connID, sendIngTask: nil}
					logger.Printf("miner %d join.\n", iMsg.connID)
					mp.miners[iMsg.connID] = m
					mp.availMiners <- m
				}
			}
		case miner := <-mp.availMiners:
			if e := mp.taskList.Front(); e != nil {
				task := e.Value.(*task)
				miner.sendIngTask = task
				sendMessage(s, task.msg, miner.connID)
				mp.taskList.Remove(e)
			} else {
				mp.availMiners <- miner
			}
		}
	}
}

func sendMessage(s lsp.Server, msg *bitcoin.Message, connID int) error {
	logger.Println("send msg:", msg.String())
	payload, err := json.Marshal(msg)
	if err != nil {
		logger.Println(err.Error())
		return err
	}
	err = s.Write(connID, payload)
	if err != nil {
		logger.Println(err.Error())
		return err
	}
	return nil
}

type internalMsg struct {
	connID  int
	isClose bool
	data    []byte
}

type client struct {
	assignMinerCnt int
	doneMinerCnt   int
	minHash        uint64
	nonce          uint64
}

type clientPool struct {
	clients map[int]*client
}

type miner struct {
	sendIngTask *task
	connID      int
}

type minerPool struct {
	miners      map[int]*miner
	availMiners chan *miner
	taskList    *list.List
}

type task struct {
	connID int
	msg    *bitcoin.Message
}

func (mp *minerPool) genTasks(connID int, message string, lower, upper uint64) *list.List {
	taskList := list.New()
	minerCnt := uint64(len(mp.miners))
	if minerCnt == 0 {
		minerCnt = 10
	}
	delta := (upper - lower + 1) / minerCnt
	if delta != 0 {
		var i uint64
		for i = 0; i < minerCnt; i++ {
			t := task{
				connID: connID,
				msg:    bitcoin.NewRequest(message, lower+delta*i, lower+delta*(i+1)-1)}
			if i == minerCnt-1 {
				t.msg.Upper = upper
			}
			taskList.PushBack(&t)
		}
	} else {
		t := task{
			connID: connID,
			msg:    bitcoin.NewRequest(message, lower, upper)}
		taskList.PushBack(&t)
	}
	return taskList
}
