package protocol

import (
	"encoding/binary"
	"math"
)

const (
	TopicLengthFieldSize = 2
	OffsetFieldSize      = 8

	TopicLengthOffset = PayloadOffset
	TopicOffset       = TopicLengthOffset + TopicLengthFieldSize
)

type RequestType uint8

const (
	RequestUnknown RequestType = iota
	RequestProduce
	RequestFetch
)

type Request struct {
	Type    RequestType
	Topic   string
	Payload []byte
	Offset  uint64
}

func EncodeRequest(request *Request) ([]byte, error) {
	switch request.Type {
	case RequestProduce:
		return encodeProduceRequest(request)

	case RequestFetch:
		return encodeFetchRequest(request)

	default:
		return nil, ErrUnknownRequestType
	}
}

func DecodeRequest(data []byte) (*Request, error) {
	if len(data) < FrameHeaderSize {
		return nil, ErrFrameTooSmall
	}

	length := binary.BigEndian.Uint32(
		data[LengthOffset:VersionOffset],
	)

	if int(length) != len(data) {
		return nil, ErrInvalidLength
	}

	if data[VersionOffset] != ProtocolVersion {
		return nil, ErrInvalidVersion
	}

	requestType := RequestType(data[TypeOffset])

	switch requestType {
	case RequestProduce:
		return decodeProduceRequest(data)

	case RequestFetch:
		return decodeFetchRequest(data)

	default:
		return nil, ErrUnknownRequestType
	}
}

func encodeProduceRequest(request *Request) ([]byte, error) {
	if len(request.Topic) == 0 || len(request.Topic) > math.MaxUint16 {
		return nil, ErrInvalidTopic
	}

	topicLength := len(request.Topic)
	frameLength := FrameHeaderSize + TopicLengthFieldSize + topicLength + len(request.Payload)
	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(data[LengthOffset:VersionOffset], uint32(frameLength))
	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(RequestProduce)
	binary.BigEndian.PutUint16(data[TopicLengthOffset:TopicOffset], uint16(topicLength))

	offsetOffset := TopicOffset + topicLength
	copy(data[TopicOffset:offsetOffset], request.Topic)
	copy(data[offsetOffset:], request.Payload)

	return data, nil
}

func decodeProduceRequest(data []byte) (*Request, error) {
	if len(data) < TopicOffset {
		return nil, ErrInvalidProduceRequest
	}

	topicLength := binary.BigEndian.Uint16(data[TopicLengthOffset:TopicOffset])
	offsetOffset := TopicOffset + int(topicLength)

	if len(data) < offsetOffset {
		return nil, ErrInvalidProduceRequest
	}

	payload := make([]byte, len(data)-offsetOffset)
	copy(payload, data[offsetOffset:])

	return &Request{
		Type:    RequestProduce,
		Topic:   string(data[TopicOffset:offsetOffset]),
		Payload: payload,
	}, nil
}

func encodeFetchRequest(request *Request) ([]byte, error) {
	if len(request.Topic) == 0 || len(request.Topic) > math.MaxUint16 {
		return nil, ErrInvalidTopic
	}

	topicLength := len(request.Topic)
	frameLength := FrameHeaderSize + TopicLengthFieldSize + topicLength + OffsetFieldSize
	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(data[LengthOffset:VersionOffset], uint32(frameLength))
	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(RequestFetch)

	binary.BigEndian.PutUint16(data[TopicLengthOffset:TopicOffset], uint16(topicLength))

	offsetOffset := TopicOffset + topicLength
	copy(data[TopicOffset:offsetOffset], request.Topic)
	PutOffset(data[offsetOffset:], request.Offset)

	return data, nil
}

func decodeFetchRequest(data []byte) (*Request, error) {
	if len(data) < TopicOffset {
		return nil, ErrInvalidFetchRequest
	}

	topicLength := binary.BigEndian.Uint16(data[TopicLengthOffset:TopicOffset])
	offsetOffset := TopicOffset + int(topicLength)

	if len(data) < offsetOffset {
		return nil, ErrInvalidFetchRequest
	}
	if len(data) != offsetOffset+OffsetFieldSize {
		return nil, ErrInvalidFetchRequest
	}

	offset := GetOffset(data[offsetOffset : offsetOffset+OffsetFieldSize])

	return &Request{
		Type:   RequestFetch,
		Topic:  string(data[TopicOffset:offsetOffset]),
		Offset: offset,
	}, nil
}

func PutOffset(data []byte, offset uint64) {
	binary.BigEndian.PutUint64(data, offset)
}

func GetOffset(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}
