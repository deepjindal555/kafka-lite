package storage

import (
	"encoding/binary"
	"io"
	"os"
)

type Segment struct {
	file *os.File
}

func OpenSegment(path string) (*Segment, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return nil, err
	}

	return &Segment{
		file: file,
	}, nil
}

func (segment *Segment) Append(record *Record) (int64, error) {
	position, err := segment.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	data := EncodeRecord(record)

	n, err := segment.file.Write(data)
	if err != nil {
		return 0, err
	}
	if n != len(data) {
		return 0, io.ErrShortWrite
	}

	return position, nil
}

func (segment *Segment) ReadAt(position int64) (*Record, error) {
	header := make([]byte, RecordHeaderSize)

	n, err := segment.file.ReadAt(header, position)
	if err != nil {
		return nil, err
	}
	if n != RecordHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}

	recordSize := binary.BigEndian.Uint32(
		header[SizeOffset:CRCOffset],
	)

	if recordSize < RecordHeaderSize {
		return nil, ErrInvalidSize
	}

	size, err := segment.Size()
	if err != nil {
		return nil, err
	}

	if position+int64(recordSize) > size {
		return nil, io.ErrUnexpectedEOF
	}

	data := make([]byte, int(recordSize))

	n, err = segment.file.ReadAt(data, position)
	if err != nil {
		return nil, err
	}
	if n != len(data) {
		return nil, io.ErrUnexpectedEOF
	}

	return DecodeRecord(data)
}

func (segment *Segment) Size() (int64, error) {
	info, err := segment.file.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

func (segment *Segment) Close() error {
	return segment.file.Close()
}
