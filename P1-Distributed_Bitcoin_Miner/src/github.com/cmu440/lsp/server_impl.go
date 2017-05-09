// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cmu440/lspnet"
)

type server struct {
	// base
	params          *Params
	conn            *lspnet.UDPConn
	nowClientId     int
	clients         chan map[int]*client // thread safe
	// recv
	recvMsgChan     chan interface{}
	// close
	isClose         bool
	toCloseChan     chan int // to notify server to close
	doneCloseChan   chan int // wait server close
	closeClientChan chan int // to notify delete closed sclient
	// debug
	goRoutineCnt    int32 // atmic
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming sclient connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		//fmt.Println("Error ResolveUDPAddr.", err.Error())
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		//fmt.Println("Error ListenUDP.", err.Error())
		return nil, err
	}
	s := &server{
		params:          params,
		clients:         make(chan map[int]*client, 1),
		toCloseChan:     make(chan int, 1),
		doneCloseChan:   make(chan int, 1),
		closeClientChan: make(chan int, MAXN),
		recvMsgChan:     make(chan interface{}, 1),
		conn:            conn,
		nowClientId:     0,
		isClose:         false,
		goRoutineCnt:    0}
	s.clients <- make(map[int]*client)
	go s.recvMsgLoop()
	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	for {
		select {
		case data := <-s.recvMsgChan:
			id, ok := data.(int)
			if ok {
				return id, nil, errors.New("The server read eror, some client has been lost.")
			}
			msg := data.(*Message)
			cli := <-s.clients
			_, ok = cli[msg.ConnID]
			s.clients <- cli
			if !ok {
				continue
				//return msg.ConnID, nil, errors.New("The client is explicitly closed.")
			} else {
				return msg.ConnID, msg.Payload, nil
			}
		}
	}
	//return 0, nil, errors.New("Unknow error.")
}

func (s *server) Write(connID int, payload []byte) error {
	cli := <-s.clients
	c, ok := cli[connID]
	s.clients <- cli
	if !ok {
		return errors.New("The connection with client has closed.")
	}
	msg := NewData(connID, c.nowSeqNum, payload)
	c.nowSeqNum++
	c.sendMsgChan <- msg
	return nil
}

func (s *server) CloseConn(connID int) error {
	cli := <-s.clients
	c, ok := cli[connID]
	s.clients <- cli
	if !ok {
		return errors.New("The connecion with client has lost.")
	}
	c.toCloseChan <- 1
	return nil
}

func (s *server) Close() error {
	cli := <-s.clients
	for _, c := range cli {
		c.toCloseChan <- 1
	}
	s.clients <- cli
	s.toCloseChan <- 1
	<-s.doneCloseChan
	s.conn.Close()
	return nil
}

func (s *server) recvMsgLoop() {
	defer atomic.AddInt32(&s.goRoutineCnt, -1)
	atomic.AddInt32(&s.goRoutineCnt, 1)

	readBytes := make([]byte, MAXN)
	for {
		select {
		case <-s.toCloseChan:
			s.isClose = true
			cli := <-s.clients
			clientCnt := len(cli)
			s.clients <- cli
			if clientCnt == 0 {
				s.doneCloseChan <- 1
				return
			}
		case connId := <-s.closeClientChan:
			// connection is lost
			cli := <-s.clients
			delete(cli, connId)
			clientCnt := len(cli)
			s.clients <- cli
			if s.isClose && clientCnt == 0 {
				s.doneCloseChan <- 1
				return
			}
		default:
			msg, rAddr, err := s.serverRecvMessage(readBytes)
			if err != nil {
				continue
			}
			if msg.Type == MsgConnect {
				if s.isClose {
					continue
				}
				s.nowClientId = s.nowClientId + 1
				c := createClient(s.params, s.conn, rAddr)
				c.connID = s.nowClientId
				cli := <-s.clients
				cli[s.nowClientId] = c
				s.clients <- cli
				ack := NewAck(s.nowClientId, 0)
				serverSendMessage(c, ack)
				go c.processMsgLoop(&s.goRoutineCnt, s.recvMsgChan, s.closeClientChan, serverSendMessage)
			} else {
				cli := <-s.clients
				c, ok := cli[msg.ConnID]
				s.clients <- cli
				if !ok {
					//fmt.Printf("client with connID %d is not exist.", msg.ConnID)
					continue
				}
				if msg.Type == MsgAck {
					c.transMsgChan <- msg
				} else if msg.Type == MsgData {
					ack := NewAck(msg.ConnID, msg.SeqNum)
					serverSendMessage(c, ack)
					c.transMsgChan <- msg
				}
			}
		}
	}
}

func serverSendMessage(c *client, msg *Message) error {
	writeBytes, err := json.Marshal(msg)
	if err != nil {
		//fmt.Println("Error server json Marshal.", err.Error())
		return err
	}
	_, err = c.conn.WriteToUDP(writeBytes, c.rAddr)
	if err != nil {
		//fmt.Println("Error server WriteToUDP.", err.Error())
		return err
	}
	//fmt.Println("Server send: " + msg.String(), time.Now())
	return nil
}

func (s *server) serverRecvMessage(readBytes []byte) (*Message, *lspnet.UDPAddr, error) {
	s.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(s.params.EpochMillis)))
	readSize, rAddr, err := s.conn.ReadFromUDP(readBytes)
	if err != nil {
		//fmt.Println("Error server ReadFromUDP.", err.Error())
		return nil, nil, err
	}
	var msg Message
	err = json.Unmarshal(readBytes[:readSize], &msg)
	if err != nil {
		//fmt.Println("Error json.Unmarshal.", err.Error())
		return nil, nil, err
	}
	//fmt.Println("Server recv:" + msg.String(), time.Now())
	return &msg, rAddr, nil
}
