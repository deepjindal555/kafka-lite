package broker

import (
	"net"
	"sync"
)

type Broker struct {
	listener net.Listener
	mu       sync.RWMutex
	topics   map[string]*Topic
}

func NewBroker(address string) (*Broker, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	return &Broker{
		listener: listener,
		topics:   make(map[string]*Topic),
	}, nil
}

func (broker *Broker) Start() error {
	for {
		connection, err := broker.listener.Accept()
		if err != nil {
			return err
		}

		go broker.handleClient(connection)
	}
}

func (broker *Broker) Close() error {
	return broker.listener.Close()
}

func (broker *Broker) CreateTopic(name string, directory string, maxSegmentSize int64) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if _, ok := broker.topics[name]; ok {
		return ErrTopicAlreadyExists
	}

	topic, err := NewTopic(name, directory, maxSegmentSize)
	if err != nil {
		return err
	}

	broker.topics[name] = topic

	return nil
}

func (broker *Broker) getTopic(name string) (*Topic, bool) {
	broker.mu.RLock()
	defer broker.mu.RUnlock()

	topic, ok := broker.topics[name]
	return topic, ok
}
