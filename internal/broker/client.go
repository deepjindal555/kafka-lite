package broker

import (
	"net"

	"kafka-lite/internal/protocol"
)

func (broker *Broker) handleClient(connection net.Conn) {
	defer connection.Close()

	for {
		frame, err := protocol.ReadFrame(connection)
		if err != nil {
			return
		}

		request, err := protocol.DecodeRequest(frame)
		if err != nil {
			return
		}

		var response *protocol.Response

		switch request.Type {
		case protocol.RequestProduce:
			response, err = broker.handleProduce(request)

		case protocol.RequestFetch:
			response, err = broker.handleFetch(request)

		case protocol.RequestMetadata:
			response, err = broker.handleMetadata(request)

		default:
			return
		}

		if err != nil {
			return
		}

		frame, err = protocol.EncodeResponse(response)
		if err != nil {
			return
		}

		if err = protocol.WriteFrame(connection, frame); err != nil {
			return
		}
	}
}
