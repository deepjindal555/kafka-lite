package storage

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	SizeFieldSize      = 4
	CRCFieldSize       = 4
	TimestampFieldSize = 8

	RecordHeaderSize = SizeFieldSize + CRCFieldSize + TimestampFieldSize

	SizeOffset      = 0
	CRCOffset       = SizeOffset + SizeFieldSize
	TimestampOffset = CRCOffset + CRCFieldSize
	PayloadOffset   = TimestampOffset + TimestampFieldSize
)

type Record struct {
	Timestamp int64
	Payload   []byte
}

func EncodeRecord(record *Record) []byte {
	totalSize := RecordHeaderSize + len(record.Payload)

	data := make([]byte, totalSize)

	binary.BigEndian.PutUint32(
		data[SizeOffset:CRCOffset],
		uint32(totalSize),
	)

	binary.BigEndian.PutUint32(
		data[CRCOffset:TimestampOffset],
		crc32.ChecksumIEEE(record.Payload),
	)

	binary.BigEndian.PutUint64(
		data[TimestampOffset:PayloadOffset],
		uint64(record.Timestamp),
	)

	copy(
		data[PayloadOffset:],
		record.Payload,
	)
	return data
}

func DecodeRecord(data []byte) (*Record, error) {
	if len(data) < RecordHeaderSize {
		return nil, ErrRecordTooSmall
	}

	recordSize := binary.BigEndian.Uint32(data[SizeOffset:CRCOffset])
	if uint32(len(data)) != recordSize {
		return nil, ErrInvalidSize
	}

	expectedCRC := binary.BigEndian.Uint32(data[CRCOffset:TimestampOffset])

	timestamp := int64(
		binary.BigEndian.Uint64(
			data[TimestampOffset:PayloadOffset],
		),
	)

	payload := make([]byte, len(data)-PayloadOffset)
	copy(payload, data[PayloadOffset:])

	if crc32.ChecksumIEEE(payload) != expectedCRC {
		return nil, ErrCRCMismatch
	}

	return &Record{
		Timestamp: timestamp,
		Payload:   payload,
	}, nil
}
