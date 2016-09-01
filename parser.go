package binlog

import (
	"errors"
)

// A BinlogParser keeps track of the current event-formatting parameters and
// parses incoming events accordingly.
type BinlogParser struct {
	format *FormatDescriptionEvent
	tables map[uint64]*TableMapEvent
}

func NewBinlogParser() *BinlogParser {
	p := new(BinlogParser)

	p.tables = make(map[uint64]*TableMapEvent)

	return p
}

func (p *BinlogParser) Parse(b []byte) (*EventContainer, error) {
	bytes := b

	h, err := parseHeader(b)
	if err != nil {
		return nil, err
	}

	b = b[EventHeaderSize:]
	eventLen := int(h.EventSize) - EventHeaderSize

	if len(b) != eventLen {
		return nil, errors.New("invalid event size")
	}

	e, err := p.parseEvent(h, b)
	if err != nil {
		return nil, err
	}

	return &EventContainer{h, e, bytes}, nil
}

func parseHeader(b []byte) (*EventHeader, error) {
	h, err := NewEventHeader(b)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// Methods that access header fields can't fail as long as IsValid() returns
// true, so they have a single return value. Methods that might fail even if
// IsValid() is true return an error value also. Methods that require data from
// the initial FORMAT_DESCRIPTION_EVENT take a BinlogFormat parameter.
func (p *BinlogParser) parseEvent(h *EventHeader, data []byte) (Event, error) {
	var e Event
	var err error

	switch h.EventType {
	// We need to catch and save any format description events, because they govern
	// how future events are parsed.
	case FORMAT_DESCRIPTION_EVENT:
		e, err = NewFormatDescriptionEvent(data)
		if err == nil {
			p.format = e.(*FormatDescriptionEvent)
		}
	case ROTATE_EVENT:
		e, err = NewRotateEvent(data)
		if err == nil {
			p.tables = make(map[uint64]*TableMapEvent) // need to reset tables after a rotate event
		}
	case TABLE_MAP_EVENT:
		e, err = NewTableMapEvent(p.format, data)
		if err == nil {
			p.tables[e.(*TableMapEvent).TableID] = e.(*TableMapEvent)
		}
	case WRITE_ROWS_EVENT_V1, DELETE_ROWS_EVENT_V1, UPDATE_ROWS_EVENT_V1:
		e, err = NewRowsEvent(p.tables, h.EventType, data)
	case QUERY_EVENT: // for transaction-grouping
		e, err = NewQueryEvent(data)
	case XID_EVENT: // for transaction-grouping; equivalent to a COMMIT
		e, err = NewXidEvent(data)
	case BEGIN_LOAD_QUERY_EVENT:
		e, err = NewBeginLoadQueryEvent(data)
	case EXECUTE_LOAD_QUERY_EVENT:
		e, err = NewExecuteLoadQueryEvent(data)
	default: // otherwise could be GTID, INTVAR, RAND, ROWS_QUERY, PRE_GA_WRITE_ROWS_EVENT, PRE_GA_UPDATE_ROWS_EVENT, PRE_GA_DELETE_ROWS_EVENT, WRITE_ROWS_EVENT_V2, UPDATE_ROWS_EVENT_V2, DELETE_ROWS_EVENT_V2 _EVENT
		e, err = NewGenericEvent(data)
	}

	if err != nil {
		return nil, &EventError{h, err.Error(), data}
	}

	return e, nil
}
