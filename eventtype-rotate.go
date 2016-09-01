package binlog

import (
	"encoding/binary"
)

// A rotate event tells us what binlog to request next.
type RotateEvent struct {
	NextPosition uint64 // position inside next binlog file
	NextFile     []byte // name of next binlog file
}

// Payload is structured as follows for MySQL v5.5:
//   8 bytes (uint64) for offset position
//   the remainder for the new log name (not zero terminated)
func NewRotateEvent(b []byte) (Event, error) {
	e := new(RotateEvent)
	i := 0

	e.NextPosition = binary.LittleEndian.Uint64(b[i : i+8])
	i = i + 8

	e.NextFile = b[i:]

	return e, nil
}
