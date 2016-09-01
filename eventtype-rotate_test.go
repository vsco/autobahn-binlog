package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	rotateEventHeader = []byte{
		// Seconds since UNIX epoch
		0x0, 0x0, 0x0, 0x0,
		// MySQL-defined binlog event type
		0x4,
		// ID of the originating MySQL server; used to filter out events in circular replication
		0x88, 0xf3, 0x0, 0x0,
		// Event size, including header, post-header, and body
		0x32, 0x0, 0x0, 0x0,
		// Position of the next event
		0x0, 0x0, 0x0, 0x0,
		// MySQL-defined binlog event flag
		0x20, 0x0,
	}
	rotateEvent = []byte{
		// New log position
		4, 0, 0, 0, 0, 0, 0, 0,
		// New log name
		109, 121, 115, 113, 108, 100, 45, 114, 101, 108, 97, 121, 45,
		98, 105, 110, 46, 48, 48, 48, 55, 52, 57,
	}
)

func TestEventHeaderTypeIsRotate(t *testing.T) {
	input := rotateEventHeader
	want := ROTATE_EVENT

	h, err := NewEventHeader(input)

	if assert.NoError(t, err) {
		assert.Equal(t, h.EventType, want)
	}
}

func TestEventCanBeParsedAsRotateEvent(t *testing.T) {
	input := rotateEvent

	_, err := NewRotateEvent(input)

	assert.NoError(t, err)
}

func TestEventCanBeCastAsRotateEvent(t *testing.T) {
	input := rotateEvent

	ev, _ := NewRotateEvent(input)
	_, ok := ev.(*RotateEvent)

	assert.True(t, ok)
}

func TestParsesRotateEventNextPositionCorrectly(t *testing.T) {
	input := rotateEvent
	want := uint64(4)

	ev, _ := NewRotateEvent(input)
	fd, _ := ev.(*RotateEvent)

	assert.EqualValues(t, fd.NextPosition, want)
}

func TestParsesRotateEventNextFileCorrectly(t *testing.T) {
	input := rotateEvent
	want := []byte("mysqld-relay-bin.000749")

	ev, _ := NewRotateEvent(input)
	fd, _ := ev.(*RotateEvent)

	assert.EqualValues(t, fd.NextFile, want)
}
