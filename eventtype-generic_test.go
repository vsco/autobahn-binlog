package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	xidEvent   = []byte{117, 77, 99, 230, 0, 0, 0, 0}
	queryEvent = []byte{
		// Slave proxy ID
		179, 208, 22, 0,
		// Execution time
		0, 0, 0, 0,
		// Database name length
		6,
		// Error code
		0, 0,
		// Status length
		26, 0,
		// Status block
		0, 0, 0, 0, 0,
		1, 0, 0, 0, 0, 0, 0, 0, 0,
		6, 3, 115, 116, 100,
		4, 33, 0, 33, 0, 192, 0,
		// Database name
		103, 108, 111, 98, 97, 108, 0,
		// Query
		66, 69, 71, 73, 78,
	}
)

func TestEventCanBeParsedAsXidEvent(t *testing.T) {
	input := xidEvent

	_, err := NewXidEvent(input)

	assert.NoError(t, err)
}

func TestEventCanBeCastAsXidEvent(t *testing.T) {
	input := xidEvent

	ev, _ := NewXidEvent(input)
	_, ok := ev.(*XidEvent)

	assert.True(t, ok)
}

func TestParsesXIDEventXIDCorrectly(t *testing.T) {
	input := xidEvent
	want := uint64(0x00000000e6634d75)

	ev, _ := NewXidEvent(input)
	x, _ := ev.(*XidEvent)

	assert.EqualValues(t, x.Xid, want)
}

func TestEventCanBeParsedAsQueryEvent(t *testing.T) {
	input := queryEvent

	_, err := NewQueryEvent(input)

	assert.NoError(t, err)
}

func TestEventCanBeCastAsQueryEvent(t *testing.T) {
	input := queryEvent

	ev, _ := NewQueryEvent(input)
	_, ok := ev.(*QueryEvent)

	assert.True(t, ok)
}

func TestParsesQueryEventSlaveProxyIDCorrectly(t *testing.T) {
	input := queryEvent
	want := uint32(0x0016d0b3)

	ev, _ := NewQueryEvent(input)
	q, _ := ev.(*QueryEvent)

	assert.EqualValues(t, q.SlaveProxyID, want)
}

func TestParsesQueryEventExecutionTimeCorrectly(t *testing.T) {
	input := queryEvent
	want := uint32(0)

	ev, _ := NewQueryEvent(input)
	q, _ := ev.(*QueryEvent)

	assert.EqualValues(t, q.ExecutionTime, want)
}

func TestParsesQueryEventDatabaseNameCorrectly(t *testing.T) {
	input := queryEvent
	want := []byte("global")

	ev, _ := NewQueryEvent(input)
	q, _ := ev.(*QueryEvent)

	assert.EqualValues(t, q.DatabaseName, want)
}

func TestParsesQueryEventQueryCorrectly(t *testing.T) {
	input := queryEvent
	want := []byte("BEGIN")

	ev, _ := NewQueryEvent(input)
	q, _ := ev.(*QueryEvent)

	assert.EqualValues(t, q.Query, want)
}
