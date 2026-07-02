package protocol

import (
	"encoding/binary"
	"math"
)

const (
	ClientInstanceLengthFieldSize = 2
	TopicLengthFieldSize          = 2
	OffsetFieldSize               = 8

	ClientInstanceLengthOffset = PayloadOffset
	ClientInstanceOffset       = ClientInstanceLengthOffset + ClientInstanceLengthFieldSize
)

type RequestType uint8

const (
	RequestUnknown RequestType = iota
	RequestProduce
	RequestFetch
)

type Request struct {
	Type           RequestType
	ClientInstance string

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
	if len(request.ClientInstance) > math.MaxUint16 {
		return nil, ErrInvalidClientInstance
	}

	if len(request.Topic) == 0 || len(request.Topic) > math.MaxUint16 {
		return nil, ErrInvalidTopic
	}

	clientInstanceLength := len(request.ClientInstance)
	topicLength := len(request.Topic)

	frameLength :=
		FrameHeaderSize +
			ClientInstanceLengthFieldSize +
			clientInstanceLength +
			TopicLengthFieldSize +
			topicLength +
			len(request.Payload)

	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(data[LengthOffset:VersionOffset], uint32(frameLength))
	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(RequestProduce)

	binary.BigEndian.PutUint16(
		data[ClientInstanceLengthOffset:ClientInstanceOffset],
		uint16(clientInstanceLength),
	)

	topicLengthOffset := ClientInstanceOffset + clientInstanceLength

	binary.BigEndian.PutUint16(
		data[topicLengthOffset:topicLengthOffset+TopicLengthFieldSize],
		uint16(topicLength),
	)

	topicOffset := topicLengthOffset + TopicLengthFieldSize
	payloadOffset := topicOffset + topicLength

	copy(data[ClientInstanceOffset:topicLengthOffset], request.ClientInstance)
	copy(data[topicOffset:payloadOffset], request.Topic)
	copy(data[payloadOffset:], request.Payload)

	return data, nil
}

func decodeProduceRequest(data []byte) (*Request, error) {
	if len(data) < ClientInstanceOffset {
		return nil, ErrInvalidProduceRequest
	}

	clientInstanceLength := binary.BigEndian.Uint16(
		data[ClientInstanceLengthOffset:ClientInstanceOffset],
	)

	topicLengthOffset := ClientInstanceOffset + int(clientInstanceLength)

	if len(data) < topicLengthOffset+TopicLengthFieldSize {
		return nil, ErrInvalidProduceRequest
	}

	topicLength := binary.BigEndian.Uint16(
		data[topicLengthOffset : topicLengthOffset+TopicLengthFieldSize],
	)

	topicOffset := topicLengthOffset + TopicLengthFieldSize
	payloadOffset := topicOffset + int(topicLength)

	if len(data) < payloadOffset {
		return nil, ErrInvalidProduceRequest
	}

	payload := make([]byte, len(data)-payloadOffset)
	copy(payload, data[payloadOffset:])

	return &Request{
		Type: RequestProduce,

		ClientInstance: string(data[ClientInstanceOffset:topicLengthOffset]),

		Topic:   string(data[topicOffset:payloadOffset]),
		Payload: payload,
	}, nil
}

func encodeFetchRequest(request *Request) ([]byte, error) {
	if len(request.ClientInstance) > math.MaxUint16 {
		return nil, ErrInvalidClientInstance
	}

	if len(request.Topic) == 0 || len(request.Topic) > math.MaxUint16 {
		return nil, ErrInvalidTopic
	}

	clientInstanceLength := len(request.ClientInstance)
	topicLength := len(request.Topic)

	frameLength :=
		FrameHeaderSize +
			ClientInstanceLengthFieldSize +
			clientInstanceLength +
			TopicLengthFieldSize +
			topicLength +
			OffsetFieldSize

	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(data[LengthOffset:VersionOffset], uint32(frameLength))
	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(RequestFetch)

	binary.BigEndian.PutUint16(
		data[ClientInstanceLengthOffset:ClientInstanceOffset],
		uint16(clientInstanceLength),
	)

	topicLengthOffset := ClientInstanceOffset + clientInstanceLength

	binary.BigEndian.PutUint16(
		data[topicLengthOffset:topicLengthOffset+TopicLengthFieldSize],
		uint16(topicLength),
	)

	topicOffset := topicLengthOffset + TopicLengthFieldSize
	offsetOffset := topicOffset + topicLength

	copy(data[ClientInstanceOffset:topicLengthOffset], request.ClientInstance)
	copy(data[topicOffset:offsetOffset], request.Topic)
	PutOffset(data[offsetOffset:], request.Offset)

	return data, nil
}

func decodeFetchRequest(data []byte) (*Request, error) {
	if len(data) < ClientInstanceOffset {
		return nil, ErrInvalidFetchRequest
	}

	clientInstanceLength := binary.BigEndian.Uint16(
		data[ClientInstanceLengthOffset:ClientInstanceOffset],
	)

	topicLengthOffset := ClientInstanceOffset + int(clientInstanceLength)

	if len(data) < topicLengthOffset+TopicLengthFieldSize {
		return nil, ErrInvalidFetchRequest
	}

	topicLength := binary.BigEndian.Uint16(
		data[topicLengthOffset : topicLengthOffset+TopicLengthFieldSize],
	)

	topicOffset := topicLengthOffset + TopicLengthFieldSize
	offsetOffset := topicOffset + int(topicLength)

	if len(data) != offsetOffset+OffsetFieldSize {
		return nil, ErrInvalidFetchRequest
	}

	offset := GetOffset(data[offsetOffset : offsetOffset+OffsetFieldSize])

	return &Request{
		Type:           RequestFetch,
		ClientInstance: string(data[ClientInstanceOffset:topicLengthOffset]),
		Topic:          string(data[topicOffset:offsetOffset]),
		Offset:         offset,
	}, nil
}

func PutOffset(data []byte, offset uint64) {
	binary.BigEndian.PutUint64(data, offset)
}

func GetOffset(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}
