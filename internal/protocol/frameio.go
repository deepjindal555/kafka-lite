package protocol

import (
	"encoding/binary"
	"io"
)

func ReadFrame(r io.Reader) ([]byte, error) {
	lengthBytes := make([]byte, LengthFieldSize)

	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, err
	}

	frameLength := binary.BigEndian.Uint32(lengthBytes)

	if frameLength < uint32(FrameHeaderSize) || frameLength > MaxFrameSize {
		return nil, ErrInvalidLength
	}

	frame := make([]byte, frameLength)

	copy(frame[:LengthFieldSize], lengthBytes)

	if _, err := io.ReadFull(r, frame[LengthFieldSize:]); err != nil {
		return nil, err
	}

	return frame, nil
}

func WriteFrame(w io.Writer, frame []byte) error {
	_, err := w.Write(frame)
	return err
}
