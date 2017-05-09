// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	//	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cmu440/lspnet"
)

const MAXN = 1024

type client struct {
	// base
	conn              *lspnet.UDPConn
	connID            int
	nowSeqNum         int
	rAddr             *lspnet.UDPAddr
	params            *Params
	// send
	sendMsgChan       chan *Message
	pendIngSendMsg    *list.List
	pendIngReSendMsg  *list.List
	unAckedMsgSnTable map[int]bool
	firstUnAckedSn    int
	// recv
	pendIngRecvMsg    *list.List
	recvMsgChan       chan interface{}
	recvMsgWindows    map[int]*Message
	firstRecvMsgSn    int
	recvMsgCnt        int
	transMsgChan      chan *Message
	// epoch
	epochTimer        *time.Timer
	noMsgEpochCnt     int
	// close
	isClose           bool
	isLost            int32 // atomic
	toCloseChan       chan int
	doneCloseChan     chan int
	closeClientChan   chan int
	//debug
	goRoutineCnt int32 // atomic
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string connIDentifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	rAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, rAddr)
	if err != nil {
		return nil, err
	}
	c := createClient(params, conn, rAddr)
	c.epochTimer.Reset(time.Millisecond * time.Duration(c.params.EpochMillis))
	newConn := NewConnect()
	clientSendMessage(c, newConn)
	readBytes := make([]byte, MAXN)
	for {
		select {
		case <-c.epochTimer.C:
			//fmt.Println("Client epoch time in connection.")
			c.noMsgEpochCnt++
			if c.noMsgEpochCnt >= c.params.EpochLimit {
				return nil, errors.New("Error Connection not established.")
			}
			clientSendMessage(c, newConn)
			c.epochTimer.Reset(time.Millisecond * time.Duration(params.EpochMillis))
		default:
			msg, rAddr, err := c.clientRecvMessage(readBytes)
			if err != nil {
				continue
			}
			c.noMsgEpochCnt = 0
			if strings.EqualFold(rAddr.String(), c.rAddr.String()) {
				if msg.Type == MsgAck && msg.SeqNum == 0 {
					//fmt.Println("Connection established.")
					c.connID = msg.ConnID
					go c.processMsgLoop(&c.goRoutineCnt, c.recvMsgChan, c.closeClientChan, clientSendMessage)
					go c.recvMsgLoop()
					return c, nil
				}
			}
		}
	}
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	select {
	case data, ok := <-c.recvMsgChan:
		if !ok {
			return nil, errors.New("Read error, the server has been lost.")
		}
		msg, ok := data.(*Message)
		if ok {
			return msg.Payload, nil
		} else {
			return nil, errors.New("Read fail, the server has been lost.")
		}
	}
}

func (c *client) Write(payload []byte) error {
	if atomic.LoadInt32(&c.isLost) != 0 {
		return errors.New("Write fail. The server has lost")
	}
	msg := NewData(c.connID, c.nowSeqNum, payload)
	c.nowSeqNum++
	c.sendMsgChan <- msg
	return nil
}

func (c *client) Close() error {
	c.toCloseChan <- 1
	<-c.doneCloseChan
	c.conn.Close()
	return nil
}

func createClient(params *Params, conn *lspnet.UDPConn, rAddr *lspnet.UDPAddr) *client {
	c := &client{
		params:            params,
		conn:              conn,
		recvMsgChan:       make(chan interface{}, 1),
		rAddr:             rAddr,
		firstRecvMsgSn:    1,
		unAckedMsgSnTable: make(map[int]bool, 1),
		epochTimer:        time.NewTimer(0),
		connID:            0,
		nowSeqNum:         1,
		recvMsgWindows:    make(map[int]*Message, 1),
		sendMsgChan:       make(chan *Message, MAXN),
		pendIngSendMsg:    list.New(),
		pendIngReSendMsg:  list.New(),
		pendIngRecvMsg:    list.New(),
		firstUnAckedSn:    1,
		recvMsgCnt:        0,
		noMsgEpochCnt:     0,
		isClose:           false,
		isLost:            0,
		goRoutineCnt:      0,
		transMsgChan:      make(chan *Message, MAXN),
		closeClientChan:   make(chan int, 1),
		toCloseChan:       make(chan int, 1),
		doneCloseChan:     make(chan int, 1)}
	return c
}

func (c *client) processRecvMsg(msg *Message, recvMsgChan chan interface{}, sendMessage func(*client, *Message) error) bool {
	c.noMsgEpochCnt = 0
	if msg.Type == MsgAck {
		if _, ok := c.unAckedMsgSnTable[msg.SeqNum]; ok {
			delete(c.unAckedMsgSnTable, msg.SeqNum)
			for e := c.pendIngReSendMsg.Front(); e != nil; e = c.pendIngReSendMsg.Front() {
				msg := e.Value.(*Message)
				if _, ok := c.unAckedMsgSnTable[msg.SeqNum]; ok {
					break
				} else {
					c.firstUnAckedSn++
					c.pendIngReSendMsg.Remove(e)
				}
			}
			c.processPendingSendMsg(sendMessage)
		}
	} else if msg.Type == MsgData {
		c.recvMsgCnt++
		if _, ok := c.recvMsgWindows[msg.SeqNum]; !ok && msg.SeqNum >= c.firstRecvMsgSn {
			c.recvMsgWindows[msg.SeqNum] = msg
			for i := c.firstRecvMsgSn; ; i++ {
				if _, ok := c.recvMsgWindows[i]; !ok {
					break
				}
				c.pendIngRecvMsg.PushBack(c.recvMsgWindows[i])
				delete(c.recvMsgWindows, i)
				c.firstRecvMsgSn++
			}
		}
	}
	if c.isClose && c.pendIngSendMsg.Len() == 0 && c.pendIngReSendMsg.Len() == 0 && len(c.unAckedMsgSnTable) == 0 && len(c.sendMsgChan) == 0 {
		return true
	}
	return false
}

func (c *client) processEpochTimer(sendMessage func(*client, *Message) error) {
	c.noMsgEpochCnt++
	if c.noMsgEpochCnt >= c.params.EpochLimit {
		atomic.StoreInt32(&c.isLost, 1)
		c.pendIngRecvMsg.PushBack(c.connID)
		return
	}
	// no data received from client
	if c.recvMsgCnt == 0 {
		ack := NewAck(c.connID, 0)
		sendMessage(c, ack)
	}
	// resend unacked message
	e := c.pendIngReSendMsg.Front()
	for i := 0; i < c.params.WindowSize && e != nil; i++ {
		msg := e.Value.(*Message)
		if _, ok := c.unAckedMsgSnTable[msg.SeqNum]; ok {
			sendMessage(c, msg)
		}
		e = e.Next()
	}
	// resend ack for last w message received
	if c.recvMsgCnt != 0 {
		i := c.firstRecvMsgSn - 1
		for j := c.params.WindowSize; j > 0 && i > 0; j-- {
			ack := NewAck(c.connID, i)
			sendMessage(c, ack)
			i--
		}
	}
	c.epochTimer.Reset(time.Millisecond * time.Duration(c.params.EpochMillis))
	return
}

func (c *client) processCloseChan() bool {
	c.isClose = true
	if c.pendIngSendMsg.Len() == 0 && c.pendIngReSendMsg.Len() == 0 && len(c.unAckedMsgSnTable) == 0 && len(c.sendMsgChan) == 0 {
		return true
	}
	return false
}

func (c *client) processSendMsg(msg *Message, sendMessage func(*client, *Message) error) {
	c.pendIngSendMsg.PushBack(msg)
	c.processPendingSendMsg(sendMessage)
}

func (c *client) processPendingSendMsg(sendMessage func(*client, *Message) error) {
	for e := c.pendIngSendMsg.Front(); e != nil; e = c.pendIngSendMsg.Front() {
		msg := e.Value.(*Message)
		if msg.SeqNum < c.firstUnAckedSn+c.params.WindowSize {
			sendMessage(c, msg)
			c.pendIngReSendMsg.PushBack(msg)
			c.unAckedMsgSnTable[msg.SeqNum] = true
			c.pendIngSendMsg.Remove(e)
		} else {
			return
		}
	}
}

func (c *client) processMsgLoop(counter *int32, recvMsgChan chan interface{}, closeClientChan chan int, sendMessage func(*client, *Message) error) {
	defer atomic.AddInt32(counter, -1)
	atomic.AddInt32(counter, 1)

	c.epochTimer.Reset(time.Millisecond * time.Duration(c.params.EpochMillis))
	for {
		if c.pendIngRecvMsg.Len() != 0 {
			e := c.pendIngRecvMsg.Front()
			data := e.Value
			if atomic.LoadInt32(&c.isLost) != 0 {
				select {
				case recvMsgChan <- data:
					c.pendIngRecvMsg.Remove(e)
				case <-c.toCloseChan:
					if c.processCloseChan() {
						closeClientChan <- c.connID
						return
					}
				}
			} else {
				select {
				case recvMsgChan <- data:
					c.pendIngRecvMsg.Remove(e)
				case <-c.toCloseChan:
					if c.processCloseChan() {
						closeClientChan <- c.connID
						return
					}
				case <-c.epochTimer.C:
					c.processEpochTimer(sendMessage)
				case msg := <-c.sendMsgChan:
					c.processSendMsg(msg, sendMessage)
				case msg := <-c.transMsgChan:
					if c.processRecvMsg(msg, recvMsgChan, sendMessage) {
						closeClientChan <- c.connID
						return
					}
				}
			}
		} else {
			if atomic.LoadInt32(&c.isLost) != 0 {
				closeClientChan <- c.connID
				return
			}
			select {
			case <-c.toCloseChan:
				if c.processCloseChan() {
					closeClientChan <- c.connID
					return
				}
			case <-c.epochTimer.C:
				c.processEpochTimer(sendMessage)
			case msg := <-c.sendMsgChan:
				c.processSendMsg(msg, sendMessage)
			case msg := <-c.transMsgChan:
				if c.processRecvMsg(msg, recvMsgChan, sendMessage) {
					closeClientChan <- c.connID
					return
				}
			}
		}
	}
}

func (c *client) recvMsgLoop() {
	defer atomic.AddInt32(&c.goRoutineCnt, -1)
	atomic.AddInt32(&c.goRoutineCnt, 1)

	readBytes := make([]byte, MAXN)
	for {
		select {
		case <-c.closeClientChan:
			c.doneCloseChan <- 1
			close(c.recvMsgChan)
			return
		default:
			msg, _, err := c.clientRecvMessage(readBytes)
			if err != nil {
				continue
			}
			if msg.Type == MsgAck {
				c.transMsgChan <- msg
			} else if msg.Type == MsgData {
				ack := NewAck(msg.ConnID, msg.SeqNum)
				clientSendMessage(c, ack)
				c.transMsgChan <- msg
			}
		}
	}
}

func clientSendMessage(c *client, msg *Message) error {
	writeBytes, err := json.Marshal(msg)
	if err != nil {
		//fmt.Println("Error client json.Marshal.", err.Error())
		return err
	}
	_, err = c.conn.Write(writeBytes)
	if err != nil {
		//fmt.Println("Error client WriteToUDP.", err.Error())
		return err
	}
	//fmt.Println("Client send:"+msg.String(), time.Now())
	return nil
}

func (c *client) clientRecvMessage(readBytes []byte) (*Message, *lspnet.UDPAddr, error) {
	c.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(c.params.EpochMillis)))
	readSize, rAddr, err := c.conn.ReadFromUDP(readBytes)
	if err != nil {
		//fmt.Println("Error client ReadFromUDP.", err.Error())
		return nil, nil, err
	}
	var msg Message
	err = json.Unmarshal(readBytes[:readSize], &msg)
	if err != nil {
		//fmt.Println("Error json.Unmarshal.", err.Error())
		return nil, nil, err
	}
	//fmt.Println("Client recv:"+msg.String(), time.Now())
	return &msg, rAddr, nil
}
