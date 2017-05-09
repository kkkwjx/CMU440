// Implementation of a MultiEchoServer. Students should write their code in this file.

package p0

import (
  "net"
  "fmt"
  "bufio"
)

const MAXN = 100

type multiEchoServer struct {
	// TODO: implement this!
  nowClientId int
  clients chan map[int]*client
  ln      *net.TCPListener
  broadChan chan []byte
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	// TODO: implement this!
  s := &multiEchoServer{
                        nowClientId: 0,
                        clients:     make(chan map[int]*client, 1),
                        broadChan:   make(chan []byte, 1)}
  s.clients <- make(map[int]*client, 1)
	return s
}

func (mes *multiEchoServer) Start(port int) error {
  addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
  if err != nil {
    return err
  }
  mes.ln, err = net.ListenTCP("tcp", addr)
  if err != nil {
    return err
  }
  go func() {
      for {
        conn, err := mes.ln.AcceptTCP()
        if err != nil {
          return
        }
        c := &client{
                      id:          mes.nowClientId,
                      conn:        conn,
                      recvChan:    make(chan []byte, 1),
                      sendChan:    make(chan []byte, MAXN),
                      toCloseSend: make(chan int, 1),
                      toCloseRecv: make(chan int, 1),
                      server:      mes}
        mes.nowClientId++
        cli :=<- mes.clients
        cli[c.id] = c
        mes.clients <- cli
        go c.recvLoop()
        go c.sendLoop()
    }
  }()
  go mes.broadCastLoop()
	return nil
}

func (mes *multiEchoServer) Close() {
	// TODO: implement this!
  mes.ln.Close()
  cli :=<- mes.clients
  for _, c := range cli {
    c.conn.Close()
    c.toCloseSend <- 1
  }
  mes.clients <- cli
  close(mes.broadChan)
}

func (mes *multiEchoServer) Count() int {
	// TODO: implement this!
  cli :=<- mes.clients
  count := len(cli)
  mes.clients <- cli
	return count
}

// TODO: add additional methods/functions below!
type client struct {
  id int
  conn *net.TCPConn
  recvChan chan []byte
  sendChan chan []byte
  server *multiEchoServer
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
    select {
    case c.server.broadChan <- []byte(msg):
      break
    case <- c.toCloseSend:
      c.toCloseRecv <- 1
      return
    }
  }
}

func (c *client) sendLoop() {
  for {
    select {
    case data, ok :=<- c.sendChan:
      if !ok {
        return
      }
      _, err := c.conn.Write(data)
      if err != nil {
        return
      }
    case <- c.toCloseRecv:
        cli :=<- c.server.clients
        delete(cli, c.id)
        c.server.clients <- cli
        return
      }
    }
}

func (mes *multiEchoServer) broadCastLoop() {
  for {
    select {
      case data, ok :=<- mes.broadChan:
        if !ok {
          return
        }
        cli :=<- mes.clients
        for _, c := range cli {
          select {
            case c.sendChan <- data:
              break
            default:
              break
          }
        }
        mes.clients <- cli
    }
  }
}
