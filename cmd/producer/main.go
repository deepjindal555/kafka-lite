package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"kafka-lite/internal/protocol"
)

const (
	address = "localhost:9092"
	topic   = "default"
)

func main() {
	connection, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")

		message, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		message = strings.TrimRight(message, "\r\n")

		request := &protocol.Request{
			Type:    protocol.RequestProduce,
			Topic:   topic,
			Payload: []byte(message),
		}

		frame, err := protocol.EncodeRequest(request)
		if err != nil {
			log.Fatal(err)
		}

		if err = protocol.WriteFrame(connection, frame); err != nil {
			log.Fatal(err)
		}

		frame, err = protocol.ReadFrame(connection)
		if err != nil {
			log.Fatal(err)
		}

		response, err := protocol.DecodeResponse(frame)
		if err != nil {
			log.Fatal(err)
		}

		if response.StatusCode != protocol.StatusOK {
			fmt.Printf("Produce failed: %v\n", response.StatusCode)
			continue
		}

		offset := protocol.GetOffset(response.Payload)

		fmt.Printf("Stored at offset %d\n", offset)
	}
}
