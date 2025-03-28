package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2"
)

func (s *BluebellServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	log.Printf("running node on %s with multi-core=%t",
		fmt.Sprintf("%s://%s", s.Network, s.Addr), s.Multicore)
	s.eng = eng
	return
}

func (s *BluebellServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	atomic.AddInt32(&s.connected, 1)
	log.Printf("now the client nums is %v", s.connected)
	return
}

func (s *BluebellServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if err != nil {
		log.Printf("error occurred on connection=%s, %v\n", c.RemoteAddr().String(), err)
	}
	atomic.AddInt32(&s.disconnected, 1)
	connected := atomic.AddInt32(&s.connected, -1)
	if connected == 0 {
		log.Printf("all %d connections are closed", s.disconnected)
	}
	return
}

func (s *BluebellServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	reader := c.(gnet.Reader)
	writer := c.(gnet.Writer)

	for {
		// Peek the first 4 bytes (header) to get the message length
		header, err := reader.Peek(4)
		if err != nil {
			if err == io.ErrShortBuffer {
				// Not enough data, exit the loop and wait for more data
				return gnet.None
			}
			log.Println("Read header error:", err)
			return gnet.None
		}

		// Extract message length
		messageLength := binary.BigEndian.Uint32(header)

		// Check if we have enough data in the buffer
		if reader.InboundBuffered() < int(messageLength+4) {
			// Not enough data for a complete message, exit the loop
			return gnet.None
		}

		// Discard the header (advance buffer)
		_, err = reader.Discard(4)
		if err != nil {
			log.Println("Discard error:", err)
			return gnet.None
		}

		// Read the message body
		message, err := reader.Next(int(messageLength))
		if err != nil {
			log.Println("Read message error:", err)
			return gnet.None
		}
		// Deserialize the message
		bluebell, err := Deserialize(message)
		fmt.Printf("req: %v\n", bluebell)

		if err != nil {
			log.Println("Failed to deserialize message:", err)
			continue
		}

		// Process the message and generate a response
		var res *BluebellResponse
		switch bluebell.Command {
		case "get":
			res = HandleGet(bluebell)
		case "set":
			res = HandleSet(bluebell)
		}
		fmt.Printf("res1: %v\n", res)
		// Serialize the response
		resBytes, err := res.Encode()

		if err != nil {
			log.Println("Failed to serialize response:", err)
			continue
		}

		// Write the response asynchronously
		err = writer.AsyncWrite(resBytes, nil)
		if err != nil {
			log.Println("Async write error:", err)
			return gnet.None
		}
	}

}
