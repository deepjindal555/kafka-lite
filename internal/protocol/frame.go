package protocol

const (
	LengthFieldSize  = 4
	VersionFieldSize = 1
	TypeFieldSize    = 1

	FrameHeaderSize = LengthFieldSize + VersionFieldSize + TypeFieldSize
	MaxFrameSize    = 16 << 20 // 16 MiB

	LengthOffset  = 0
	VersionOffset = LengthOffset + LengthFieldSize
	TypeOffset    = VersionOffset + VersionFieldSize
	PayloadOffset = TypeOffset + TypeFieldSize
)

const (
	ProtocolVersion uint8 = 1
)
