package client

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/valyala/bytebufferpool"
)

var timerPool = sync.Pool{
	New: func() interface{} {
		return time.NewTimer(0) // 初始化定时器
	},
}
var HuaHuoLsmCli *HuaHuoLsmClient

type HuaHuoLsmClient struct {
	Clients map[string]*Client
	Ready   bool
}

func LsmCliInit() {
	HuaHuoLsmCli = &HuaHuoLsmClient{
		Clients: make(map[string]*Client),
		Ready:   true,
	}
}

type Client struct {
	ServerAddr string
	ServerPort int
	Conn       net.Conn
	ResponseCh chan []byte
	Buffer     *bytebufferpool.ByteBuffer
	Status     bool
}

func New(serverAddr string, serverPort int) *Client {
	s := fmt.Sprintf("%s:%d", serverAddr, serverPort)
	if HuaHuoLsmCli.Clients[s] != nil {
		return HuaHuoLsmCli.Clients[s]
	}
	return &Client{
		ServerAddr: serverAddr,
		ServerPort: serverPort,
		Buffer:     bytebufferpool.Get(),
		Status:     true,
	}
}

func (c *Client) Start() error {
	statusCh := make(chan bool) // 创建一个通道用于通知启动状态

	go func() {
		defer close(statusCh) // 确保在函数结束时关闭通道

		c.ResponseCh = make(chan []byte, 1024)
		log.Println("Starting huacache client...")
		addr := fmt.Sprintf("%s:%d", c.ServerAddr, c.ServerPort)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Printf("Connection failed: %v\n", err)
			statusCh <- false
			return
		}
		c.Conn = conn
		defer conn.Close()

		log.Println("Client started successfully, waiting to receive messages...")
		statusCh <- true // 通知调用者启动成功
		reader := bufio.NewReader(c.Conn)

		for {
			inBuffer := make([]byte, 1024)
			n, err := reader.Read(inBuffer)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Printf("Error reading from server: %v\n", err)
				return
			}

			if n > 0 {
				c.Buffer.Write(inBuffer[:n])
			}

			for c.Buffer.Len() >= 4 {
				header := c.Buffer.Bytes()[:4]
				messageLength := binary.BigEndian.Uint32(header)

				if uint32(c.Buffer.Len()) < messageLength+4 {
					break
				}
				c.Buffer.B = c.Buffer.B[4:] // Skip length bytes
				message := c.Buffer.B[:messageLength]
				c.Buffer.B = c.Buffer.B[messageLength:] // Adjust the buffer

				c.ResponseCh <- message
			}
		}
		c.Close()
		log.Println("Client is shutting down " + time.Now().Format("2006-01-02 15:04:05"))
	}()
	if success := <-statusCh; success {
		log.Println("Client has started successfully.")
		// 这里可以进行其他操作，比如开始处理消息
	} else {
		log.Println("Client failed to start.")
	}
	return nil // 返回通道
}

func (c *Client) sendRequestToServer(request *Bluebell) error {
	if c.Conn == nil {
		return errors.New("connection has not been established")
	}
	data, err := request.Encode()
	if err != nil {
		log.Printf("Failed to serialize message: %v", err)
		return err
	}
	_, err = c.Conn.Write(data)
	return err
}

func (c *Client) waitForResponseWithTimeout(timeout time.Duration) (*BluebellResponse, error) {
	timer := timerPool.Get().(*time.Timer)
	timer.Reset(timeout)
	defer func() {
		timer.Stop()
		timerPool.Put(timer)
	}()

	select {
	case response := <-c.ResponseCh:
		res, err := DeserializeResponse(response)
		if err != nil {
			log.Printf("Error during response deserialization: %v", err)
			return nil, err
		}
		return res, nil
	case <-timer.C:
		return nil, errors.New("timeout waiting for response")
	}
}

func (c *Client) Close() error {
	if c.Conn != nil {
		err := c.Conn.Close()
		c.Conn = nil // 清理连接
		if err != nil {
			log.Printf("Failed to close connection: %v", err)
			return err
		}
	}
	bytebufferpool.Put(c.Buffer)
	log.Println("Client is shutting down " + time.Now().Format("2006-01-02 15:04:05"))
	return nil
}
