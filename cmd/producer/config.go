package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
)

const (
	defaultMinMessageSize = 1
	defaultMaxMessageSize = 65536
	defaultSeed           = int64(0)
)

type WorkloadMode string

const (
	WorkloadSequential WorkloadMode = "sequential"
	WorkloadFixed      WorkloadMode = "fixed"
	WorkloadRandom     WorkloadMode = "random"
)

type CLIConfig struct {
	Automatic bool

	Producer ProducerConfig
	Workload WorkloadConfig
}

type WorkloadConfig struct {
	Messages uint64
	Mode     WorkloadMode

	MessageSize    int
	MinMessageSize int
	MaxMessageSize int

	Rate uint64
	Seed int64
}

func parseProducerConfig(args []string) (*CLIConfig, error) {
	var (
		config CLIConfig

		maxBatchRecordsFlag uint
		maxBatchBytesFlag   uint
		modeFlag            string
	)

	config.Producer = ProducerConfig{
		MaxBatchRecords: maxBatchRecords,
		MaxBatchBytes:   maxBatchBytes,
		Linger:          linger,
		PrintBatchAcks:  true,
	}

	config.Workload = WorkloadConfig{
		Mode:           WorkloadSequential,
		MinMessageSize: defaultMinMessageSize,
		MaxMessageSize: defaultMaxMessageSize,
		Seed:           defaultSeed,
	}

	maxBatchRecordsFlag = uint(config.Producer.MaxBatchRecords)
	maxBatchBytesFlag = uint(config.Producer.MaxBatchBytes)
	modeFlag = string(config.Workload.Mode)

	flags := flag.NewFlagSet("producer", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	flags.BoolVar(&config.Automatic, "automatic", false, "generate records automatically")

	flags.Uint64Var(&config.Workload.Messages, "messages", 0, "number of messages to produce in automatic mode")
	flags.StringVar(&modeFlag, "mode", string(WorkloadSequential), "workload mode: sequential, fixed, or random")
	flags.Uint64Var(&config.Workload.Rate, "rate", 0, "target message rate in messages/second (0 = unlimited)")
	flags.Int64Var(&config.Workload.Seed, "seed", defaultSeed, "random seed for reproducible payload generation")

	flags.IntVar(&config.Workload.MessageSize, "message-size", 0, "payload size in bytes for fixed mode")
	flags.IntVar(&config.Workload.MinMessageSize, "min-message-size", defaultMinMessageSize, "minimum payload size in bytes for random mode")
	flags.IntVar(&config.Workload.MaxMessageSize, "max-message-size", defaultMaxMessageSize, "maximum payload size in bytes for random mode")

	flags.UintVar(&maxBatchRecordsFlag, "max-batch-records", uint(maxBatchRecords), "maximum records per producer batch")
	flags.UintVar(&maxBatchBytesFlag, "max-batch-bytes", uint(maxBatchBytes), "maximum producer batch size in bytes")
	flags.DurationVar(&config.Producer.Linger, "linger", linger, "producer linger duration")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			flags.SetOutput(os.Stdout)
			fmt.Fprintln(os.Stdout, "Usage of producer:")
			flags.PrintDefaults()
			flags.SetOutput(io.Discard)
		}
		return nil, err
	}

	setFlags := make(map[string]bool)
	flags.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = true
	})

	config.Workload.Mode = WorkloadMode(modeFlag)

	if err := validateProducerConfig(&config, setFlags, maxBatchRecordsFlag, maxBatchBytesFlag); err != nil {
		return nil, err
	}

	config.Producer.MaxBatchRecords = uint32(maxBatchRecordsFlag)
	config.Producer.MaxBatchBytes = uint32(maxBatchBytesFlag)
	config.Producer.PrintBatchAcks = !config.Automatic

	return &config, nil
}

func validateProducerConfig(config *CLIConfig, setFlags map[string]bool, maxBatchRecordsFlag, maxBatchBytesFlag uint) error {
	automaticOnlyFlags := []string{
		"messages",
		"mode",
		"message-size",
		"min-message-size",
		"max-message-size",
		"rate",
		"seed",
	}

	if maxBatchRecordsFlag == 0 || maxBatchRecordsFlag > math.MaxUint32 {
		return errors.New("--max-batch-records must be between 1 and 4294967295")
	}

	if maxBatchBytesFlag == 0 || maxBatchBytesFlag > math.MaxUint32 {
		return errors.New("--max-batch-bytes must be between 1 and 4294967295")
	}

	if config.Producer.Linger < 0 {
		return errors.New("--linger must not be negative")
	}

	if !config.Automatic {
		for _, name := range automaticOnlyFlags {
			if setFlags[name] {
				return fmt.Errorf("--%s requires --automatic", name)
			}
		}

		return nil
	}

	if config.Workload.Messages == 0 {
		return errors.New("--messages must be greater than 0 in automatic mode")
	}

	switch config.Workload.Mode {
	case WorkloadSequential:
		if setFlags["message-size"] || setFlags["min-message-size"] || setFlags["max-message-size"] {
			return errors.New("sequential mode rejects message size flags")
		}

	case WorkloadFixed:
		if !setFlags["message-size"] {
			return errors.New("fixed mode requires --message-size")
		}
		if config.Workload.MessageSize <= 0 {
			return errors.New("--message-size must be greater than 0")
		}
		if setFlags["min-message-size"] || setFlags["max-message-size"] {
			return errors.New("fixed mode rejects random size flags")
		}

	case WorkloadRandom:
		if setFlags["message-size"] {
			return errors.New("random mode rejects --message-size")
		}
		if config.Workload.MinMessageSize <= 0 {
			return errors.New("--min-message-size must be greater than 0")
		}
		if config.Workload.MaxMessageSize <= 0 {
			return errors.New("--max-message-size must be greater than 0")
		}
		if config.Workload.MinMessageSize > config.Workload.MaxMessageSize {
			return errors.New("--min-message-size must be less than or equal to --max-message-size")
		}

	default:
		return fmt.Errorf("unknown automatic mode %q", config.Workload.Mode)
	}

	return nil
}
