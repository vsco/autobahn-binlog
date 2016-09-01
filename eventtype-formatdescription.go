package binlog

import (
	"encoding/binary"
	"errors"
)

// A format description event is the first event for binlog-version 4;
// it describes how the following events are structured.
type FormatDescriptionEvent struct {
	BinlogVersion          uint16
	ServerVersion          []byte
	CreationTimestamp      uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []byte
}

// Payload is structured as follows for MySQL v5.5:
//   2 bytes (uint16) for binlog version
//   50 bytes for server version string (padded with '\0's)
//   4 bytes (uint32) for created timestamp. Note that this value may be
//     unpopulated
//   1 byte (uint8) for total header size, where total header size = common header
//     size + extra headers size
//   1 byte per event type for event's fixed length data size. Note that unknown
//     events doesn't have an entry
//   27 bytes for events' fixed size length (one uint8 entry per event type except
//     unknown events)
func NewFormatDescriptionEvent(b []byte) (Event, error) {
	e := new(FormatDescriptionEvent)
	i := 0

	// Version of this binlog format (2 bytes); should always be 4
	e.BinlogVersion = binary.LittleEndian.Uint16(b[i : i+2])
	i = i + 2

	// Version of the MySQL server that created the binlog (string[50])
	e.ServerVersion = make([]byte, 50)
	copy(e.ServerVersion, b[i:i+50])
	i = i + 50

	// Seconds since Unix epoch when the binlog was created (4 bytes)
	e.CreationTimestamp = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// Length of the binlog event header of following events; should always match
	// const EventHeaderSize (1 byte)
	e.EventHeaderLength = b[i]
	i = i + 1
	if e.EventHeaderLength != byte(EventHeaderSize) {
		return nil, errors.New("invalid event header length")
	}

	// An array indexed by binlogeventtype - 1 to extract the length of the event-specific header (string[p])
	e.EventTypeHeaderLengths = b[i:]

	return e, nil
}
