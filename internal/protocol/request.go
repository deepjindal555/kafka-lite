package protocol

import (
	"encoding/binary"
	"kafka-lite/internal/batch"
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
	RequestMetadata
)

type ProduceRequest struct {
	Topic string
	Batch *batch.RecordBatch
}

type FetchRequest struct {
	Topic  string
	Offset uint64
}

type MetadataRequest struct {
}

type Request struct {
	Type           RequestType
	ClientInstance string

	Produce  *ProduceRequest
	Fetch    *FetchRequest
	Metadata *MetadataRequest
}

func EncodeRequest(request *Request) ([]byte, error) {
	switch request.Type {
	case RequestProduce:
		return encodeProduceRequest(request)

	case RequestFetch:
		return encodeFetchRequest(request)

	case RequestMetadata:
		return encodeMetadataRequest(request)

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

	case RequestMetadata:
		return decodeMetadataRequest(data)

	default:
		return nil, ErrUnknownRequestType
	}
}

func encodeProduceRequest(request *Request) ([]byte, error) {
	if request.Produce == nil {
		return nil, ErrNilProduceRequest
	}

	if len(request.ClientInstance) > math.MaxUint16 {
		return nil, ErrInvalidClientInstance
	}

	if len(request.Produce.Topic) == 0 || len(request.Produce.Topic) > math.MaxUint16 {
		return nil, ErrInvalidTopic
	}

	batchData, err := batch.EncodeBatch(request.Produce.Batch)
	if err != nil {
		return nil, err
	}

	clientInstanceLength := len(request.ClientInstance)
	topicLength := len(request.Produce.Topic)

	frameLength :=
		FrameHeaderSize +
			ClientInstanceLengthFieldSize +
			clientInstanceLength +
			TopicLengthFieldSize +
			topicLength +
			len(batchData)

	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(
		data[LengthOffset:VersionOffset],
		uint32(frameLength),
	)

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
	copy(data[topicOffset:payloadOffset], request.Produce.Topic)
	copy(data[payloadOffset:], batchData)

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
	batchOffset := topicOffset + int(topicLength)

	if len(data) < batchOffset {
		return nil, ErrInvalidProduceRequest
	}

	recordBatch, err := batch.DecodeBatch(data[batchOffset:])
	if err != nil {
		return nil, err
	}

	return &Request{
		Type:           RequestProduce,
		ClientInstance: string(data[ClientInstanceOffset:topicLengthOffset]),

		Produce: &ProduceRequest{
			Topic: string(data[topicOffset:batchOffset]),
			Batch: recordBatch,
		},
	}, nil
}

func encodeFetchRequest(request *Request) ([]byte, error) {
	if request.Fetch == nil {
		return nil, ErrNilFetchRequest
	}

	if len(request.ClientInstance) > math.MaxUint16 {
		return nil, ErrInvalidClientInstance
	}

	if len(request.Fetch.Topic) == 0 || len(request.Fetch.Topic) > math.MaxUint16 {
		return nil, ErrInvalidTopic
	}

	clientInstanceLength := len(request.ClientInstance)
	topicLength := len(request.Fetch.Topic)

	frameLength :=
		FrameHeaderSize +
			ClientInstanceLengthFieldSize +
			clientInstanceLength +
			TopicLengthFieldSize +
			topicLength +
			OffsetFieldSize

	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(
		data[LengthOffset:VersionOffset],
		uint32(frameLength),
	)

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
	copy(data[topicOffset:offsetOffset], request.Fetch.Topic)
	PutOffset(data[offsetOffset:], request.Fetch.Offset)

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

	return &Request{
		Type:           RequestFetch,
		ClientInstance: string(data[ClientInstanceOffset:topicLengthOffset]),

		Fetch: &FetchRequest{
			Topic:  string(data[topicOffset:offsetOffset]),
			Offset: GetOffset(data[offsetOffset : offsetOffset+OffsetFieldSize]),
		},
	}, nil
}

func encodeMetadataRequest(request *Request) ([]byte, error) {
	if request.Metadata == nil {
		return nil, ErrNilMetadataRequest
	}

	if len(request.ClientInstance) > math.MaxUint16 {
		return nil, ErrInvalidClientInstance
	}

	clientInstanceLength := len(request.ClientInstance)

	frameLength := FrameHeaderSize + ClientInstanceLengthFieldSize + clientInstanceLength
	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(
		data[LengthOffset:VersionOffset],
		uint32(frameLength),
	)

	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(RequestMetadata)

	binary.BigEndian.PutUint16(
		data[ClientInstanceLengthOffset:ClientInstanceOffset],
		uint16(clientInstanceLength),
	)

	copy(
		data[ClientInstanceOffset:],
		request.ClientInstance,
	)

	return data, nil
}

func decodeMetadataRequest(data []byte) (*Request, error) {
	if len(data) < ClientInstanceOffset {
		return nil, ErrInvalidMetadataRequest
	}

	clientInstanceLength := binary.BigEndian.Uint16(
		data[ClientInstanceLengthOffset:ClientInstanceOffset],
	)

	clientInstanceOffset := ClientInstanceOffset
	end := clientInstanceOffset + int(clientInstanceLength)

	if len(data) != end {
		return nil, ErrInvalidMetadataRequest
	}

	return &Request{
		Type:           RequestMetadata,
		ClientInstance: string(data[clientInstanceOffset:end]),

		Metadata: &MetadataRequest{},
	}, nil
}

func PutOffset(data []byte, offset uint64) {
	binary.BigEndian.PutUint64(data, offset)
}

func GetOffset(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}
