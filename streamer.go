package binlog

import (
	"errors"
)

// Stream handles the routing of events/errors from the follower to client channels.
type Streamer struct {
	ch  chan *EventContainer
	ech chan error
	err error
}

func (s *Streamer) GetEvent() (*EventContainer, error) {
	if s.err != nil {
		return nil, errors.New("last sync failed")
	}

	select {
	case c := <-s.ch:
		return c, nil
	case s.err = <-s.ech:
		return nil, s.err
	}
}

func (s *Streamer) Close() {
	s.closeWithError(errors.New("last sync failed"))
}

func (s *Streamer) closeWithError(err error) {
	if err == nil {
		err = errors.New("sync was closed")
	}
	select {
	case s.ech <- err:
	default:
	}
}

func newStreamer() *Streamer {
	s := new(Streamer)

	s.ch = make(chan *EventContainer, 1024)
	s.ech = make(chan error, 4)

	return s
}
