package batch

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	RecordSizeFieldSize      = 4
	RecordCRCFieldSize       = 4
	RecordTimestampFieldSize = 8

	RecordHeaderSize = RecordSizeFieldSize + RecordCRCFieldSize + RecordTimestampFieldSize

	RecordSizeOffset      = 0
	RecordCRCOffset       = RecordSizeOffset + RecordSizeFieldSize
	RecordTimestampOffset = RecordCRCOffset + RecordCRCFieldSize
	RecordPayloadOffset   = RecordTimestampOffset + RecordTimestampFieldSize
)

type Record struct {
	Timestamp int64
	Payload   []byte
}

func EncodeRecord(record *Record) []byte {
	totalSize := RecordHeaderSize + len(record.Payload)

	data := make([]byte, totalSize)

	binary.BigEndian.PutUint32(
		data[RecordSizeOffset:RecordCRCOffset],
		uint32(totalSize),
	)

	binary.BigEndian.PutUint32(
		data[RecordCRCOffset:RecordTimestampOffset],
		crc32.ChecksumIEEE(record.Payload),
	)

	binary.BigEndian.PutUint64(
		data[RecordTimestampOffset:RecordPayloadOffset],
		uint64(record.Timestamp),
	)

	copy(
		data[RecordPayloadOffset:],
		record.Payload,
	)
	return data
}

func DecodeRecord(data []byte) (*Record, error) {
	if len(data) < RecordHeaderSize {
		return nil, ErrRecordTooSmall
	}

	recordSize := binary.BigEndian.Uint32(data[RecordSizeOffset:RecordCRCOffset])
	if uint32(len(data)) != recordSize {
		return nil, ErrInvalidRecordSize
	}

	expectedCRC := binary.BigEndian.Uint32(data[RecordCRCOffset:RecordTimestampOffset])

	timestamp := int64(
		binary.BigEndian.Uint64(
			data[RecordTimestampOffset:RecordPayloadOffset],
		),
	)

	payload := make([]byte, len(data)-RecordPayloadOffset)
	copy(payload, data[RecordPayloadOffset:])

	if crc32.ChecksumIEEE(payload) != expectedCRC {
		return nil, ErrRecordCRCMismatch
	}

	return &Record{
		Timestamp: timestamp,
		Payload:   payload,
	}, nil
}

func DecodeRecordSize(header []byte) (uint32, error) {
	if len(header) < RecordHeaderSize {
		return 0, ErrRecordTooSmall
	}

	recordSize := binary.BigEndian.Uint32(
		header[RecordSizeOffset:RecordCRCOffset],
	)

	if recordSize < RecordHeaderSize {
		return 0, ErrInvalidRecordSize
	}

	return recordSize, nil
}

func RecordPositions(data []byte, batchPosition int64) ([]int64, error) {
	positions := make([]int64, 0)

	position := batchPosition + BatchHeaderSize

	for len(data) > 0 {
		recordSize, err := DecodeRecordSize(data)
		if err != nil {
			return nil, err
		}

		if int(recordSize) > len(data) {
			return nil, ErrInvalidRecordSize
		}

		positions = append(positions, position)

		position += int64(recordSize)
		data = data[int(recordSize):]
	}

	return positions, nil
}
