package main

import (
	"fmt"
	"math/rand"
	"time"

	"kafka-lite/internal/batch"
)

type workloadGenerator struct {
	config WorkloadConfig
	random *rand.Rand

	nextSequence uint64
}

func newWorkloadGenerator(config WorkloadConfig) *workloadGenerator {
	return &workloadGenerator{
		config: config,
		random: rand.New(rand.NewSource(config.Seed)),
	}
}

func (generator *workloadGenerator) NextPayload() []byte {
	switch generator.config.Mode {
	case WorkloadSequential:
		generator.nextSequence++
		return fmt.Appendf(nil, "msg-%d", generator.nextSequence)

	case WorkloadFixed:
		return generator.randomPayload(generator.config.MessageSize)

	case WorkloadRandom:
		sizeRange := generator.config.MaxMessageSize - generator.config.MinMessageSize + 1
		size := generator.config.MinMessageSize + generator.random.Intn(sizeRange)
		return generator.randomPayload(size)

	default:
		panic("unknown workload mode")
	}
}

const alphanumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func (generator *workloadGenerator) randomPayload(size int) []byte {
	payload := make([]byte, size)

	for i := range payload {
		payload[i] = alphanumeric[generator.random.Intn(len(alphanumeric))]
	}

	return payload
}

func (generator *workloadGenerator) nextTopic(topics []string) string {
	if len(topics) == 0 {
		panic("no topics configured")
	}

	return topics[generator.random.Intn(len(topics))]
}

func automaticProducerLoop(producer *Producer, config WorkloadConfig) error {
	generator := newWorkloadGenerator(config)
	interval := rateInterval(config.Rate)

	for i := uint64(0); i < config.Messages; i++ {
		if i > 0 && interval > 0 {
			time.Sleep(interval)
		}

		record := &batch.Record{
			Timestamp: time.Now().UnixNano(),
			Payload:   generator.NextPayload(),
		}

		if err := producer.Produce(generator.nextTopic(producer.config.Topics), record); err != nil {
			return err
		}
	}

	return nil
}

func rateInterval(rate uint64) time.Duration {
	if rate == 0 {
		return 0
	}

	interval := time.Second / time.Duration(rate)
	if interval <= 0 {
		return 0
	}

	return interval
}
