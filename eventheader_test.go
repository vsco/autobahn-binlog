package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	eventHeader = []byte{0x52, 0x52, 0xe9, 0x53, 0xf, 0x88, 0xf3, 0x0, 0x0, 0x66, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x35, 0x2e, 0x31, 0x2e, 0x36, 0x33, 0x2d, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1b, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x53, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2}
)

func TestEventHeaderBadLength(t *testing.T) {
	input := []byte("imshort")

	_, err := NewEventHeader(input)

	assert.Error(t, err)
}

type eventHeaderTest struct {
	input []byte
	want  interface{}
}

var eventHeaderTimestampTests = []eventHeaderTest{
	{eventHeader, uint32(0x53e95252)},
	{rotateEventHeader, uint32(0x0)},
}

func TestEventHeaderTimestamp(t *testing.T) {
	for _, tst := range eventHeaderTimestampTests {
		h, err := NewEventHeader(tst.input)

		if assert.NoError(t, err) {
			assert.Equal(t, h.Timestamp, tst.want)
		}
	}
}

var eventHeaderTypeTests = []eventHeaderTest{
	{rotateEventHeader, ROTATE_EVENT},
	{formatEventHeader, FORMAT_DESCRIPTION_EVENT},
}

func TestEventHeaderType(t *testing.T) {
	for _, tyt := range eventHeaderTypeTests {
		h, err := NewEventHeader(tyt.input)

		if assert.NoError(t, err) {
			assert.Equal(t, h.EventType, tyt.want)
		}
	}
}

var eventHeaderServerIdTests = []eventHeaderTest{
	{rotateEventHeader, uint32(0xf388)},
}

func TestEventHeaderServerId(t *testing.T) {
	for _, sit := range eventHeaderServerIdTests {
		h, err := NewEventHeader(sit.input)

		if assert.NoError(t, err) {
			assert.Equal(t, h.ServerId, sit.want)
		}
	}
}

var eventHeaderEventSizeTests = []eventHeaderTest{
	{rotateEventHeader, uint32(50)},
}

func TestEventHeaderEventSize(t *testing.T) {
	for _, est := range eventHeaderEventSizeTests {
		h, err := NewEventHeader(est.input)

		if assert.NoError(t, err) {
			assert.Equal(t, h.EventSize, est.want)
		}
	}
}

var eventHeaderLogPosTests = []eventHeaderTest{
	{rotateEventHeader, uint32(0x0)},
}

func TestEventHeaderLogPos(t *testing.T) {
	for _, lpt := range eventHeaderLogPosTests {
		h, err := NewEventHeader(lpt.input)

		if assert.NoError(t, err) {
			assert.Equal(t, h.LogPos, lpt.want)
		}
	}
}

var eventHeaderFlagsTests = []eventHeaderTest{
	{rotateEventHeader, uint16(0x20)},
}

func TestEventHeaderFlags(t *testing.T) {
	for _, ft := range eventHeaderFlagsTests {
		h, err := NewEventHeader(ft.input)

		if assert.NoError(t, err) {
			assert.Equal(t, h.Flags, ft.want)
		}
	}
}
