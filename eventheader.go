package binlog

import (
	"encoding/binary"
	"errors"
)

type EventHeader struct {
	Timestamp uint32
	EventType EventType
	ServerId  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

// Returns a parsed EventHeader if the bytes provided are valid.
// (This requires binlog version > 1.)
func NewEventHeader(b []byte) (*EventHeader, error) {
	h := new(EventHeader)

	if len(b) < EventHeaderSize {
		return nil, errors.New("error parsing event header: header size too short")
	}

	i := 0

	// Seconds since UNIX epoch
	h.Timestamp = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// MySQL-defined binlog event type
	h.EventType = EventType(b[i])
	i = i + 1

	// ID of the originating MySQL server; used to filter out events in circular replication
	h.ServerId = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// Event size, including header, post-header, and body
	eventSize := binary.LittleEndian.Uint32(b[i:])
	if eventSize < uint32(EventHeaderSize) {
		return nil, errors.New("error parsing event header: stated event size too short")
	}
	h.EventSize = eventSize
	i = i + 4

	// Position of the next event
	h.LogPos = binary.LittleEndian.Uint32(b[i:])
	i = i + 4

	// MySQL-defined binlog event flag
	h.Flags = binary.LittleEndian.Uint16(b[i:])
	i = i + 2

	return h, nil
}
