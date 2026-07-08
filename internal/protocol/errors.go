package protocol

import "errors"

var (
	ErrFrameTooSmall  = errors.New("frame too small")
	ErrInvalidLength  = errors.New("invalid frame length")
	ErrInvalidVersion = errors.New("invalid protocol version")
	ErrInvalidTopic   = errors.New("invalid topic")

	ErrInvalidClientInstance = errors.New("invalid client instance")

	ErrUnknownRequestType  = errors.New("unknown request type")
	ErrUnknownResponseType = errors.New("unknown response type")

	ErrNilProduceRequest = errors.New("nil produce request")
	ErrNilFetchRequest   = errors.New("nil fetch request")

	ErrInvalidProduceRequest = errors.New("invalid produce request")
	ErrInvalidFetchRequest   = errors.New("invalid fetch request")

	ErrInvalidProduceResponse = errors.New("invalid produce response")
	ErrInvalidFetchResponse   = errors.New("invalid fetch response")
)
