package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	tableMapEventHeader = []byte{
		// Seconds since UNIX epoch
		0x0, 0x0, 0x2, 0x1,
		// MySQL-defined binlog event type
		0x13,
		// ID of the originating MySQL server; used to filter out events in circular replication
		0x88, 0xf3, 0x0, 0x0,
		// Event size, including header, post-header, and body
		0x4A, 0x0, 0x0, 0x0,
		// Position of the next event
		0x0, 0x0, 0x0, 0x0,
		// MySQL-defined binlog event flag
		0x0, 0x0,
	}
	tableMapEvent = []byte{
		// Table ID
		76, 0, 0, 0, 0, 0,
		// Flags
		1, 0,
		// Database name length
		8,
		// Database name
		115, 104, 97, 114, 100, 55, 54, 55, 0,
		// Table name length
		30,
		// Table name
		99, 97, 109, 101, 114, 97, 95, 117, 112, 108,
		111, 97, 100, 95, 105, 110, 100, 101, 120, 95,
		115, 117, 109, 109, 97, 114, 121, 95, 118, 51, 0,
		// Number of columns
		2,
		3, 2,
		// Metadata size
		0,
		// Null bits
		2,
	}
)

func TestEventHeaderTypeIsTableMap(t *testing.T) {
	input := tableMapEventHeader
	want := TABLE_MAP_EVENT

	h, err := NewEventHeader(input)

	if assert.NoError(t, err) {
		assert.Equal(t, h.EventType, want)
	}
}

func TestEventCanBeParsedAsTableMapEvent(t *testing.T) {
	input := tableMapEvent

	fde, _ := NewFormatDescriptionEvent(formatDescriptionEvent)
	_, err := NewTableMapEvent(fde.(*FormatDescriptionEvent), input)

	assert.NoError(t, err)
}

func TestEventCanBeCastAsTableMapEvent(t *testing.T) {
	input := tableMapEvent

	fde, _ := NewFormatDescriptionEvent(formatDescriptionEvent)
	ev, _ := NewTableMapEvent(fde.(*FormatDescriptionEvent), input)
	_, ok := ev.(*TableMapEvent)

	assert.True(t, ok)
}

func TestParsesTableMapEventTableIDCorrectly(t *testing.T) {
	input := tableMapEvent
	want := uint64(76)

	fde, _ := NewFormatDescriptionEvent(formatDescriptionEvent)
	ev, _ := NewTableMapEvent(fde.(*FormatDescriptionEvent), input)
	tme, _ := ev.(*TableMapEvent)

	assert.EqualValues(t, tme.TableID, want)
}

func TestParsesTableMapEventFlagsCorrectly(t *testing.T) {
	input := tableMapEvent
	want := uint64(1)

	fde, _ := NewFormatDescriptionEvent(formatDescriptionEvent)
	ev, _ := NewTableMapEvent(fde.(*FormatDescriptionEvent), input)
	tme, _ := ev.(*TableMapEvent)

	assert.EqualValues(t, tme.Flags, want)
}

func TestParsesTableMapEventDatabaseNameCorrectly(t *testing.T) {
	input := tableMapEvent
	want := "shard767"

	fde, _ := NewFormatDescriptionEvent(formatDescriptionEvent)
	ev, _ := NewTableMapEvent(fde.(*FormatDescriptionEvent), input)
	tme, _ := ev.(*TableMapEvent)

	assert.EqualValues(t, tme.DatabaseName, want)
}

func TestParsesTableMapEventTableNameCorrectly(t *testing.T) {
	input := tableMapEvent
	want := "camera_upload_index_summary_v3"

	fde, _ := NewFormatDescriptionEvent(formatDescriptionEvent)
	ev, _ := NewTableMapEvent(fde.(*FormatDescriptionEvent), input)
	tme, _ := ev.(*TableMapEvent)

	assert.EqualValues(t, tme.TableName, want)
}
