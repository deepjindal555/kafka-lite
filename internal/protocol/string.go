package protocol

import "fmt"

func (requestType RequestType) String() string {
	switch requestType {
	case RequestUnknown:
		return "Unknown"

	case RequestProduce:
		return "Produce"

	case RequestFetch:
		return "Fetch"

	default:
		return fmt.Sprintf("Unknown(%d)", requestType)
	}
}

func (responseType ResponseType) String() string {
	switch responseType {
	case ResponseUnknown:
		return "Unknown"

	case ResponseProduce:
		return "Produce"

	case ResponseFetch:
		return "Fetch"

	default:
		return fmt.Sprintf("Unknown(%d)", responseType)
	}
}

func (status StatusCode) String() string {
	switch status {
	case StatusOK:
		return "OK"

	case StatusOffsetNotFound:
		return "OffsetNotFound"

	case StatusInternalError:
		return "InternalError"

	case StatusTopicNotFound:
		return "TopicNotFound"

	default:
		return fmt.Sprintf("Unknown(%d)", status)
	}
}
