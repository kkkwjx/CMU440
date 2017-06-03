// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
  "fmt"
  "bufio"
  "net"
  "strings"
)

const MAXN = 500

type keyValueServer struct {
	// TODO: implement this!
	nowClientId int
	clients     chan map[int]*client
	ln          *net.TCPListener
	broadChan   chan []byte
	kvChan      chan int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	s := &keyValueServer{
		nowClientId: 0,
    kvChan:      make(chan int, 1),
		clients:     make(chan map[int]*client, 1),
		broadChan:   make(chan []byte, 1)}
  s.kvChan <- 1
	s.clients <- make(map[int]*client, 1)
	return s
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	kvs.ln, err = net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := kvs.ln.AcceptTCP()
			if err != nil {
				return
			}
			c := &client{
				id:          kvs.nowClientId,
				conn:        conn,
				recvChan:    make(chan []byte, 1),
				sendChan:    make(chan []byte, MAXN),
				toCloseSend: make(chan int, 1),
				toCloseRecv: make(chan int, 1),
				server:      kvs}
			kvs.nowClientId++
			cli := <-kvs.clients
			cli[c.id] = c
			kvs.clients <- cli
			go c.recvLoop()
			go c.sendLoop()
		}
	}()
	go kvs.broadCastLoop()
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.ln.Close()
	cli := <-kvs.clients
	for _, c := range cli {
		c.conn.Close()
		c.toCloseSend <- 1
	}
	kvs.clients <- cli
	close(kvs.broadChan)
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	cli := <-kvs.clients
	count := len(cli)
	kvs.clients <- cli
	return count
}

// TODO: add additional methods/functions below!
type client struct {
	id          int
	conn        *net.TCPConn
	recvChan    chan []byte
	sendChan    chan []byte
	server      *keyValueServer
	toCloseRecv chan int
	toCloseSend chan int
}

func (c *client) recvLoop() {
	br := bufio.NewReader(c.conn)
	for {
		msg, err := br.ReadBytes('\n')
		if err != nil {
			c.toCloseRecv <- 1
			return
		}
		sendOrNot, sendMsg := c.server.parse(string(msg))
		if !sendOrNot {
			select {
			case <-c.toCloseSend:
				c.toCloseRecv <- 1
				return
			default:
				break
			}
		} else {
			select {
			case c.server.broadChan <- []byte(sendMsg):
				break
			case <-c.toCloseSend:
				c.toCloseRecv <- 1
				return
			}
		}
	}
}

func (c *client) sendLoop() {
	for {
		select {
		case data, ok := <-c.sendChan:
			if !ok {
				return
			}
			_, err := c.conn.Write(data)
			if err != nil {
				return
			}
		case <-c.toCloseRecv:
			cli := <-c.server.clients
			delete(cli, c.id)
			c.server.clients <- cli
			return
		}
	}
}

func (kvs *keyValueServer) broadCastLoop() {
	for {
		select {
		case data, ok := <-kvs.broadChan:
			if !ok {
				return
			}
			cli := <-kvs.clients
			for _, c := range cli {
				select {
				case c.sendChan <- data:
					break
				default:
					break
				}
			}
			kvs.clients <- cli
		}
	}
}

func (kvs *keyValueServer) parse(msg string) (bool, string) {
	words := strings.Split(msg, ",")
	if len(words) == 3 && strings.EqualFold(words[0], "put") {
    words[2] = strings.TrimSpace(words[2])
    <- kvs.kvChan
		put(words[1], []byte(words[2]))
		kvs.kvChan <- 1
		return false, ""
	} else if len(words) == 2 && strings.EqualFold(words[0], "get") {
    words[1] = strings.TrimSpace(words[1])
		<- kvs.kvChan
		value := get(words[1])
		res := fmt.Sprintf("%v,%v\n", words[1], string(value))
		kvs.kvChan <- 1
		return true, res
	}
	return false, ""
}
