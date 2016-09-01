package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	formatEventHeader = []byte{
		// Seconds since UNIX epoch
		0x52, 0x52, 0xe9, 0x53,
		// MySQL-defined binlog event type
		0xf,
		// ID of the originating MySQL server; used to filter out events in circular replication
		0x88, 0xf3, 0x0, 0x0,
		// Event size, including header, post-header, and body
		0x67, 0x0, 0x0, 0x0,
		// Position of the next event
		0x0, 0x0, 0x0, 0x0,
		// MySQL-defined binlog event flag
		0x0, 0x0,
	}
	formatDescriptionEvent = []byte{
		// Binlog version
		4, 0,
		// Server version
		53, 46, 53, 46, 51, 52, 45, 51, 50, 46,
		48, 45, 108, 111, 103, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		// Created timestamp
		0, 0, 0, 0,
		// Total header size
		19,
		// Fixed length data size per event type
		56, 13, 0, 8, 0, 18, 0, 4, 4, 4, 4, 18, 0, 0, 84, 0, 4,
		26, 8, 0, 0, 0, 8, 8, 8, 2, 0,
	}
)

func TestEventHeaderTypeIsFormatDescription(t *testing.T) {
	input := formatEventHeader
	want := FORMAT_DESCRIPTION_EVENT

	h, err := NewEventHeader(input)

	if assert.NoError(t, err) {
		assert.Equal(t, h.EventType, want)
	}
}

func TestEventCanBeParsedAsFormatDescriptionEvent(t *testing.T) {
	input := formatDescriptionEvent

	_, err := NewFormatDescriptionEvent(input)

	assert.NoError(t, err)
}

func TestEventCanBeCastAsFormatDescriptionEvent(t *testing.T) {
	input := formatDescriptionEvent

	ev, _ := NewFormatDescriptionEvent(input)
	_, ok := ev.(*FormatDescriptionEvent)

	assert.True(t, ok)
}

func TestParsesFormatDescriptionEventBinlogVersionCorrectly(t *testing.T) {
	input := formatDescriptionEvent
	want := uint16(4)

	ev, _ := NewFormatDescriptionEvent(input)
	fd, _ := ev.(*FormatDescriptionEvent)

	assert.EqualValues(t, fd.BinlogVersion, want)
}

func TestParsesFormatDescriptionEventServerVersionCorrectly(t *testing.T) {
	input := formatDescriptionEvent
	want := make([]byte, 50)
	copy(want, "5.5.34-32.0-log")

	ev, _ := NewFormatDescriptionEvent(input)
	fd, _ := ev.(*FormatDescriptionEvent)

	assert.EqualValues(t, fd.ServerVersion, want)
}

func TestParsesFormatDescriptionEventCreationTimestampCorrectly(t *testing.T) {
	input := formatDescriptionEvent
	want := uint32(0)

	ev, _ := NewFormatDescriptionEvent(input)
	fd, _ := ev.(*FormatDescriptionEvent)

	assert.EqualValues(t, fd.CreationTimestamp, want)
}
