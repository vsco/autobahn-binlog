package binlog

// An EventContainer represents a single event from a raw MySQL binlog stream.
// A Streamer receives these events through a Follower and processes them.
type EventContainer struct {
	Header *EventHeader // parsed event header
	Event  Event        // parsed event body
	Bytes  []byte       // event body as raw bytes
}

// Event represents the real data in an EventContainer, in a possibly-parsed form.
type Event interface {
}

type EventError struct {
	Header *EventHeader
	Err    string
	Data   []byte
}

func (e *EventError) Error() string {
	return e.Err
}
