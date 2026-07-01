package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"kafka-lite/internal/protocol"
)

const (
	address      = "localhost:9092"
	topic        = "default"
	retryTimeout = 100 * time.Millisecond
)

func main() {
	connection, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()

	var nextOffset uint64

	for {
		request := &protocol.Request{
			Type:   protocol.RequestFetch,
			Topic:  topic,
			Offset: nextOffset,
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

		switch response.StatusCode {
		case protocol.StatusOK:
			fmt.Printf("[%d] %s\n", nextOffset, string(response.Payload))
			nextOffset++

		case protocol.StatusOffsetNotFound:
			time.Sleep(retryTimeout)

		default:
			log.Fatalf("fetch failed: %v", response.StatusCode)
		}
	}
}
