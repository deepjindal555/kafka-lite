package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	OffsetFieldSize   = 8
	PositionFieldSize = 8

	IndexEntrySize = OffsetFieldSize + PositionFieldSize

	OffsetOffset   = 0
	PositionOffset = OffsetOffset + OffsetFieldSize
)

type Index struct {
	BaseOffset uint64
	file       *os.File
}

func OpenIndex(path string, baseOffset uint64) (*Index, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return nil, err
	}

	return &Index{
		BaseOffset: baseOffset,
		file:       file,
	}, nil
}

func (index *Index) Write(offset uint64, position int64) error {
	entry := make([]byte, IndexEntrySize)

	binary.BigEndian.PutUint64(
		entry[OffsetOffset:PositionOffset],
		offset,
	)

	binary.BigEndian.PutUint64(
		entry[PositionOffset:],
		uint64(position),
	)

	relativeOffset := offset - index.BaseOffset
	n, err := index.file.WriteAt(
		entry,
		int64(relativeOffset)*IndexEntrySize,
	)
	if err != nil {
		return err
	}

	if n != IndexEntrySize {
		return io.ErrShortWrite
	}

	return nil
}

func (index *Index) Lookup(offset uint64) (int64, error) {
	entry := make([]byte, IndexEntrySize)

	relativeOffset := offset - index.BaseOffset
	n, err := index.file.ReadAt(
		entry,
		int64(relativeOffset)*IndexEntrySize,
	)

	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, ErrOffsetNotFound
		}
		return 0, err
	}

	if n != IndexEntrySize {
		return 0, io.ErrUnexpectedEOF
	}

	storedOffset := binary.BigEndian.Uint64(entry[OffsetOffset:PositionOffset])

	if storedOffset != offset {
		return 0, ErrCorruptIndex
	}

	position := int64(
		binary.BigEndian.Uint64(entry[PositionOffset:]),
	)

	return position, nil
}

func (index *Index) Size() (int64, error) {
	info, err := index.file.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

func (index *Index) Close() error {
	return index.file.Close()
}
