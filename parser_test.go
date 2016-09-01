package binlog

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestParsingShortHeaderFails(t *testing.T) {
	input := []byte("imshort")
	_, err := parseHeader(input)
	assert.Error(t, err)
}

func TestParsingProperHeaderWorks(t *testing.T) {
	h, err := parseHeader(formatEventHeader)
	want := &EventHeader{
		Timestamp: uint32(1407799890),
		EventType: FORMAT_DESCRIPTION_EVENT,
		ServerId:  uint32(62344),
		EventSize: uint32(103),
		LogPos:    uint32(0),
		Flags:     uint16(0),
	}

	assert.NoError(t, err)
	assert.Equal(t, h, want)
}

type BinlogParserTestSuite struct {
	suite.Suite
	parser *BinlogParser
}

func TestBinlogParserTestSuite(t *testing.T) {
	suite.Run(t, new(BinlogParserTestSuite))
}

func (suite *BinlogParserTestSuite) TestParsingTableMapEventWorksCorrectly() {
	input := bytes.Join([][]byte{tableMapEventHeader, tableMapEvent}, []byte{})

	ev, err := suite.parser.Parse(input)
	if assert.NoError(suite.T(), err) {
		headerWant := &EventHeader{
			Timestamp: uint32(0x1020000),
			EventType: TABLE_MAP_EVENT,
			ServerId:  uint32(62344),
			EventSize: uint32(74),
			LogPos:    uint32(0),
			Flags:     uint16(0),
		}

		eventWant := &TableMapEvent{
			TableID:        uint64(76),
			Flags:          uint16(1),
			DatabaseName:   []byte("shard767"),
			TableName:      []byte("camera_upload_index_summary_v3"),
			ColumnCount:    uint64(2),
			ColumnTypes:    []byte{0x3, 0x2},
			ColumnMetadata: []uint16{0x0, 0x0},
			NullBitVector:  []byte{0x2},
		}

		assert.Equal(suite.T(), ev.Header, headerWant)
		assert.Equal(suite.T(), ev.Event, eventWant)
		assert.Equal(suite.T(), ev.Bytes, input)

		tablesWant := map[uint64]*TableMapEvent{
			eventWant.TableID: eventWant,
		}
		assert.Equal(suite.T(), suite.parser.tables, tablesWant)
	}
}

func (suite *BinlogParserTestSuite) TestParsingRotateEventWorksCorrectly() {
	input := bytes.Join([][]byte{rotateEventHeader, rotateEvent}, []byte{})

	ev, err := suite.parser.Parse(input)
	if assert.NoError(suite.T(), err) {
		headerWant := &EventHeader{
			Timestamp: uint32(0),
			EventType: ROTATE_EVENT,
			ServerId:  uint32(62344),
			EventSize: uint32(50),
			LogPos:    uint32(0),
			Flags:     uint16(32),
		}

		eventWant := &RotateEvent{
			NextPosition: uint64(4),
			NextFile:     []byte("mysqld-relay-bin.000749"),
		}

		assert.Equal(suite.T(), ev.Header, headerWant)
		assert.Equal(suite.T(), ev.Event, eventWant)
		assert.Equal(suite.T(), ev.Bytes, input)
		assert.Equal(suite.T(), suite.parser.tables, make(map[uint64]*TableMapEvent))
	}
}

func (suite *BinlogParserTestSuite) TestParsingFormatDescriptionEventWorksCorrectly() {
	input := bytes.Join([][]byte{formatEventHeader, formatDescriptionEvent}, []byte{})
	ev, err := suite.parser.Parse(input)
	if assert.NoError(suite.T(), err) {
		headerWant := &EventHeader{
			Timestamp: uint32(1407799890),
			EventType: FORMAT_DESCRIPTION_EVENT,
			ServerId:  uint32(62344),
			EventSize: uint32(103),
			LogPos:    uint32(0),
			Flags:     uint16(0),
		}

		serverVersionWant := make([]byte, 50)
		copy(serverVersionWant, "5.5.34-32.0-log")

		eventWant := &FormatDescriptionEvent{
			BinlogVersion:          uint16(4),
			ServerVersion:          serverVersionWant,
			CreationTimestamp:      uint32(0),
			EventHeaderLength:      uint8(19),
			EventTypeHeaderLengths: []byte{56, 13, 0, 8, 0, 18, 0, 4, 4, 4, 4, 18, 0, 0, 84, 0, 4, 26, 8, 0, 0, 0, 8, 8, 8, 2, 0},
		}

		assert.Equal(suite.T(), ev.Header, headerWant)
		assert.Equal(suite.T(), ev.Event, eventWant)
		assert.Equal(suite.T(), ev.Bytes, input)
		assert.Equal(suite.T(), suite.parser.format, eventWant)
	}
}

func (suite *BinlogParserTestSuite) SetupSuite() {
	suite.parser = NewBinlogParser()
}
