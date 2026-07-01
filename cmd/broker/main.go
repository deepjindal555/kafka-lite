package main

import (
	"log"

	"kafka-lite/internal/broker"
)

const (
	address       = ":9092"
	dataDirectory = "data/default"
	// maxSegmentSize = 10 << 20 // 10 MiB
	maxSegmentSize = 200
)

func main() {
	broker, err := broker.NewBroker(address)
	if err != nil {
		log.Fatal(err)
	}

	defer broker.Close()

	if err = broker.CreateTopic("default", dataDirectory, maxSegmentSize); err != nil {
		log.Fatal(err)
	}

	log.Println("Broker listening on", address)

	if err = broker.Start(); err != nil {
		log.Fatal(err)
	}
}
