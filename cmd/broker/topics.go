package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type TopicConfig struct {
	Name       string `yaml:"name"`
	Partitions int    `yaml:"partitions"`
}

func loadTopics(filename string) ([]TopicConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var file struct {
		Topics []TopicConfig `yaml:"topics"`
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	if err := dec.Decode(&file); err != nil {
		return nil, err
	}

	if err := validateTopics(file.Topics); err != nil {
		return nil, err
	}

	return file.Topics, nil
}

func validateTopics(topics []TopicConfig) error {
	if len(topics) == 0 {
		return errors.New("no topics configured")
	}

	seen := make(map[string]struct{}, len(topics))

	for _, topic := range topics {
		topicName := strings.TrimSpace(topic.Name)
		if topicName == "" {
			return errors.New("topic name must not be empty")
		}

		if topic.Partitions <= 0 {
			return fmt.Errorf(
				"topic %q must have at least one partition",
				topicName,
			)
		}

		if _, exists := seen[topicName]; exists {
			return fmt.Errorf(
				"duplicate topic %q",
				topicName,
			)
		}

		seen[topicName] = struct{}{}
	}

	return nil
}
