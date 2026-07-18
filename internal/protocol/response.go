package protocol

import "encoding/binary"

const (
	StatusCodeFieldSize = 1
	TopicCountFieldSize = 2

	RecordCountFieldSize  = 4
	PartitionFieldSize    = 4
	RecordLengthFieldSize = 4

	ResponseBodyOffset = PayloadOffset + StatusCodeFieldSize
)

type ResponseType uint8

const (
	ResponseUnknown ResponseType = iota
	ResponseProduce
	ResponseFetch
	ResponseMetadata
)

type StatusCode uint8

const (
	StatusOK StatusCode = iota
	StatusOffsetNotFound
	StatusBatchTooLarge
	StatusInternalError
	StatusTopicNotFound
)

type ProduceResponse struct {
	BaseOffset uint64
}

type PartitionRecord struct {
	Partition uint32
	Record    []byte
}

type FetchResponse struct {
	Records []PartitionRecord
}

type TopicMetadata struct {
	Name           string
	PartitionCount uint32
}

type MetadataResponse struct {
	Topics []TopicMetadata
}

type Response struct {
	Type       ResponseType
	StatusCode StatusCode

	Produce  *ProduceResponse
	Fetch    *FetchResponse
	Metadata *MetadataResponse
}

func EncodeResponse(response *Response) ([]byte, error) {
	switch response.Type {
	case ResponseProduce:
		return encodeProduceResponse(response)

	case ResponseFetch:
		return encodeFetchResponse(response)

	case ResponseMetadata:
		return encodeMetadataResponse(response)

	default:
		return nil, ErrUnknownResponseType
	}
}

func DecodeResponse(data []byte) (*Response, error) {
	if len(data) < FrameHeaderSize+StatusCodeFieldSize {
		return nil, ErrFrameTooSmall
	}

	length := int(binary.BigEndian.Uint32(
		data[LengthOffset:VersionOffset],
	))

	if length != len(data) {
		return nil, ErrInvalidLength
	}

	if data[VersionOffset] != ProtocolVersion {
		return nil, ErrInvalidVersion
	}

	responseType := ResponseType(data[TypeOffset])

	switch responseType {
	case ResponseProduce:
		return decodeProduceResponse(data)

	case ResponseFetch:
		return decodeFetchResponse(data)

	case ResponseMetadata:
		return decodeMetadataResponse(data)

	default:
		return nil, ErrUnknownResponseType
	}
}

func encodeProduceResponse(response *Response) ([]byte, error) {
	if response.Produce == nil {
		return nil, ErrInvalidProduceResponse
	}

	payloadLength := 0
	if response.StatusCode == StatusOK {
		payloadLength = OffsetFieldSize
	}

	frameLength := FrameHeaderSize + StatusCodeFieldSize + payloadLength
	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(data[LengthOffset:VersionOffset], uint32(frameLength))
	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(ResponseProduce)
	data[PayloadOffset] = byte(response.StatusCode)

	if response.StatusCode == StatusOK {
		PutOffset(
			data[ResponseBodyOffset:ResponseBodyOffset+OffsetFieldSize],
			response.Produce.BaseOffset,
		)
	}
	return data, nil
}

func decodeProduceResponse(data []byte) (*Response, error) {
	statusCode := StatusCode(data[PayloadOffset])

	if statusCode == StatusOK {
		if len(data) != ResponseBodyOffset+OffsetFieldSize {
			return nil, ErrInvalidProduceResponse
		}

		return &Response{
			Type:       ResponseProduce,
			StatusCode: statusCode,
			Produce: &ProduceResponse{
				BaseOffset: GetOffset(data[ResponseBodyOffset:]),
			},
		}, nil
	}

	if len(data) != ResponseBodyOffset {
		return nil, ErrInvalidProduceResponse
	}

	return &Response{
		Type:       ResponseProduce,
		StatusCode: statusCode,
		Produce:    &ProduceResponse{},
	}, nil
}

func encodeFetchResponse(response *Response) ([]byte, error) {
	if response.Fetch == nil {
		return nil, ErrInvalidFetchResponse
	}

	payloadLength := RecordCountFieldSize

	for _, record := range response.Fetch.Records {
		payloadLength += PartitionFieldSize + RecordLengthFieldSize + len(record.Record)
	}

	frameLength := FrameHeaderSize + StatusCodeFieldSize + payloadLength
	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(data[LengthOffset:VersionOffset], uint32(frameLength))
	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(ResponseFetch)
	data[PayloadOffset] = byte(response.StatusCode)

	offset := ResponseBodyOffset

	binary.BigEndian.PutUint32(
		data[offset:offset+RecordCountFieldSize],
		uint32(len(response.Fetch.Records)),
	)

	offset += RecordCountFieldSize

	for _, record := range response.Fetch.Records {
		binary.BigEndian.PutUint32(
			data[offset:offset+PartitionFieldSize],
			record.Partition,
		)
		offset += PartitionFieldSize

		binary.BigEndian.PutUint32(
			data[offset:offset+RecordLengthFieldSize],
			uint32(len(record.Record)),
		)
		offset += RecordLengthFieldSize

		copy(data[offset:offset+len(record.Record)], record.Record)
		offset += len(record.Record)
	}

	return data, nil
}

func decodeFetchResponse(data []byte) (*Response, error) {
	if len(data) < ResponseBodyOffset+RecordCountFieldSize {
		return nil, ErrInvalidFetchResponse
	}

	offset := ResponseBodyOffset

	recordCount := binary.BigEndian.Uint32(
		data[offset : offset+RecordCountFieldSize],
	)

	offset += RecordCountFieldSize

	records := make([]PartitionRecord, 0, recordCount)

	for range recordCount {
		if len(data) < offset+PartitionFieldSize+RecordLengthFieldSize {
			return nil, ErrInvalidFetchResponse
		}

		partition := binary.BigEndian.Uint32(
			data[offset : offset+PartitionFieldSize],
		)
		offset += PartitionFieldSize

		recordLength := int(binary.BigEndian.Uint32(
			data[offset : offset+RecordLengthFieldSize],
		))
		offset += RecordLengthFieldSize

		if len(data) < offset+recordLength {
			return nil, ErrInvalidFetchResponse
		}

		record := make([]byte, recordLength)
		copy(record, data[offset:offset+recordLength])
		offset += recordLength

		records = append(records, PartitionRecord{
			Partition: partition,
			Record:    record,
		})
	}

	return &Response{
		Type:       ResponseFetch,
		StatusCode: StatusCode(data[PayloadOffset]),
		Fetch: &FetchResponse{
			Records: records,
		},
	}, nil
}

func encodeMetadataResponse(response *Response) ([]byte, error) {
	if response.Metadata == nil {
		return nil, ErrInvalidMetadataResponse
	}

	payloadLength := TopicCountFieldSize
	for _, topic := range response.Metadata.Topics {
		payloadLength += TopicLengthFieldSize + len(topic.Name) + PartitionCountFieldSize
	}

	frameLength := FrameHeaderSize + StatusCodeFieldSize + payloadLength
	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(data[LengthOffset:VersionOffset], uint32(frameLength))
	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(ResponseMetadata)
	data[PayloadOffset] = byte(response.StatusCode)

	offset := ResponseBodyOffset

	binary.BigEndian.PutUint16(
		data[offset:offset+TopicCountFieldSize],
		uint16(len(response.Metadata.Topics)),
	)

	offset += TopicCountFieldSize

	for _, topic := range response.Metadata.Topics {
		binary.BigEndian.PutUint16(data[offset:offset+TopicLengthFieldSize], uint16(len(topic.Name)))
		offset += TopicLengthFieldSize

		copy(data[offset:offset+len(topic.Name)], topic.Name)
		offset += len(topic.Name)

		binary.BigEndian.PutUint32(data[offset:offset+PartitionCountFieldSize], topic.PartitionCount)
		offset += PartitionCountFieldSize
	}

	return data, nil
}

func decodeMetadataResponse(data []byte) (*Response, error) {
	if len(data) < ResponseBodyOffset+TopicCountFieldSize {
		return nil, ErrInvalidMetadataResponse
	}

	offset := ResponseBodyOffset

	topicCount := binary.BigEndian.Uint16(data[offset : offset+TopicCountFieldSize])
	offset += TopicCountFieldSize

	topics := make([]TopicMetadata, 0, topicCount)

	for range topicCount {
		if len(data) < offset+TopicLengthFieldSize {
			return nil, ErrInvalidMetadataResponse
		}

		topicLength := int(binary.BigEndian.Uint16(data[offset : offset+TopicLengthFieldSize]))
		offset += TopicLengthFieldSize

		if len(data) < offset+topicLength+PartitionCountFieldSize {
			return nil, ErrInvalidMetadataResponse
		}

		topicName := string(data[offset : offset+topicLength])
		offset += topicLength

		partitionCount := binary.BigEndian.Uint32(data[offset : offset+PartitionCountFieldSize])
		offset += PartitionCountFieldSize

		topics = append(topics, TopicMetadata{
			Name:           topicName,
			PartitionCount: partitionCount,
		})
	}

	if offset != len(data) {
		return nil, ErrInvalidMetadataResponse
	}

	return &Response{
		Type:       ResponseMetadata,
		StatusCode: StatusCode(data[PayloadOffset]),
		Metadata: &MetadataResponse{
			Topics: topics,
		},
	}, nil
}
