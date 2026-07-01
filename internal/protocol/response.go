package protocol

import "encoding/binary"

const (
	StatusCodeFieldSize = 1

	ResponsePayloadOffset = PayloadOffset + StatusCodeFieldSize
)

type ResponseType uint8

const (
	ResponseUnknown ResponseType = iota
	ResponseProduce
	ResponseFetch
)

type StatusCode uint8

const (
	StatusOK StatusCode = iota
	StatusOffsetNotFound
	StatusInternalError
	StatusTopicNotFound
)

type Response struct {
	Type       ResponseType
	StatusCode StatusCode
	Payload    []byte
}

func EncodeResponse(response *Response) ([]byte, error) {
	switch response.Type {
	case ResponseProduce:
		return encodeProduceResponse(response)

	case ResponseFetch:
		return encodeFetchResponse(response)

	default:
		return nil, ErrUnknownResponseType
	}
}

func DecodeResponse(data []byte) (*Response, error) {
	if len(data) < FrameHeaderSize+StatusCodeFieldSize {
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

	responseType := ResponseType(data[TypeOffset])

	switch responseType {
	case ResponseProduce:
		return decodeProduceResponse(data)

	case ResponseFetch:
		return decodeFetchResponse(data)

	default:
		return nil, ErrUnknownResponseType
	}
}

func encodeProduceResponse(response *Response) ([]byte, error) {
	if response.StatusCode == StatusOK && len(response.Payload) != 8 {
		return nil, ErrInvalidProduceResponse
	}

	frameLength := FrameHeaderSize + StatusCodeFieldSize + len(response.Payload)
	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(data[LengthOffset:VersionOffset], uint32(frameLength))
	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(ResponseProduce)
	data[PayloadOffset] = byte(response.StatusCode)
	copy(data[ResponsePayloadOffset:], response.Payload)

	return data, nil
}

func decodeProduceResponse(data []byte) (*Response, error) {
	payload := make([]byte, len(data)-ResponsePayloadOffset)
	copy(payload, data[ResponsePayloadOffset:])

	if StatusCode(data[PayloadOffset]) == StatusOK && len(payload) != 8 {
		return nil, ErrInvalidProduceResponse
	}

	return &Response{
		Type:       ResponseProduce,
		StatusCode: StatusCode(data[PayloadOffset]),
		Payload:    payload,
	}, nil
}

func encodeFetchResponse(response *Response) ([]byte, error) {
	frameLength := FrameHeaderSize + StatusCodeFieldSize + len(response.Payload)
	data := make([]byte, frameLength)

	binary.BigEndian.PutUint32(data[LengthOffset:VersionOffset], uint32(frameLength))
	data[VersionOffset] = ProtocolVersion
	data[TypeOffset] = byte(ResponseFetch)
	data[PayloadOffset] = byte(response.StatusCode)
	copy(data[ResponsePayloadOffset:], response.Payload)

	return data, nil
}

func decodeFetchResponse(data []byte) (*Response, error) {
	payload := make([]byte, len(data)-ResponsePayloadOffset)
	copy(payload, data[ResponsePayloadOffset:])

	return &Response{
		Type:       ResponseFetch,
		StatusCode: StatusCode(data[PayloadOffset]),
		Payload:    payload,
	}, nil
}
