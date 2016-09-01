package binlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Follower represents an entity that masquerades to MySQL as a replication
// follower (or "slave").
type Follower struct {
	wg              sync.WaitGroup
	m               sync.Mutex
	c               *Conn
	followerID         uint32
	hostname        string
	host            string
	port            uint16
	user            string
	password        string
	masterID        uint32
	parser          *BinlogParser
	NextPosition    Position
	running         bool
	semiSyncEnabled bool
	stopChan        chan struct{}
}

// NewFollower returns a new Follower. The followerId passed in must be unique
// among all followers of a particular MySQL leader; if any two followers' server
// IDs collide, the master will stop sending packets to one of the two.
func NewFollower(followerId uint32) *Follower {
	f := &Follower{
		followerID:         followerId,
		masterID:        0,
		parser:          NewBinlogParser(),
		running:         false,
		semiSyncEnabled: false,
		stopChan:        make(chan struct{}, 1),
	}

	return f
}

// GetLeaderPosition returns the leader's current binlog filename and position.
func (f *Follower) GetLeaderPosition() (filename string, position uint32, err error) {
	var result *Result
	result, err = f.c.execute("SHOW MASTER STATUS")
	if err != nil {
		return
	}

	var positionValue int64

	filename, err = result.GetString(0, 0)
	if err != nil {
		return "", 0, err
	}

	positionValue, err = result.GetInt64(0, 1)
	if err != nil {
		return "", 0, err
	}

	return filename, uint32(positionValue), nil
}

// Hostname returns the hostname that the Follower will register to the leader as.
func (f *Follower) Hostname() string {
	if f.hostname == "" {
		h, _ := os.Hostname()
		return h
	}

	return f.hostname
}

func (f *Follower) checkExec() error {
	if f.running {
		return errors.New("Sync is running, must close first")
	} else if f.c == nil {
		return errors.New("Follower is not registered.")
	}

	return nil
}

// RegisterFollower closes any existing replication session, then registers the
// Follower to the leader.
func (f *Follower) RegisterFollower(host string, port uint16, user string, password string) error {
	f.Close()

	f.host = host
	f.port = port
	f.user = user
	f.password = password

	err := f.registerFollower()
	if err != nil {
		f.Close()
	}

	return err
}

func (f *Follower) registerFollower() error {
	var err error
	f.c, err = NewConn(f.host, f.port, f.user, f.password, "")
	if err != nil {
		return err
	}

	var r *Result
	if r, err = f.c.execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"); err != nil {
		return err
	} else {
		str, _ := r.GetString(0, 1)
		if str != "" {
			if _, err = f.c.execute(`SET @master_binlog_checksum='NONE'`); err != nil {
				return err
			}
		}
	}

	if err = f.writeRegisterFollowerCommand(); err != nil {
		return err
	}

	if _, err = f.c.readOKPacket(); err != nil {
		return err
	}

	return nil
}

// startStream starts streaming binlog events using the settings already set.
func (f *Follower) startStream() *Streamer {
	f.running = true
	f.stopChan = make(chan struct{}, 1)

	str := newStreamer()

	f.wg.Add(1)
	go f.parseEventsTo(str)

	return str
}

func (f *Follower) StartSync(binlogFile string, binlogPos uint32) (*Streamer, error) {
	pos := Position{binlogFile, binlogPos}

	f.m.Lock()
	defer f.m.Unlock()

	if err := f.checkExec(); err != nil {
		return nil, err
	}

	// Always start from position >= 4
	if pos.Pos < 4 {
		pos.Pos = 4
	}

	err := f.writeBinlogDumpCommand(pos)
	if err != nil {
		return nil, err
	}

	return f.startStream(), nil
}

// writeBinlogDumpCommand requests that the leader start a binlog network stream.
func (f *Follower) writeBinlogDumpCommand(p Position) error {
	f.c.resetSequence()

	data := makeBinlogDumpCommand(p, f.followerID)

	return f.c.writePacket(data)
}

func makeBinlogDumpCommand(p Position, followerID uint32) []byte {
	b := make([]byte, 4+1+4+2+4+len(p.Name))

	i := 4
	b[i] = COM_BINLOG_DUMP
	i++

	binary.LittleEndian.PutUint32(b[i:], p.Pos)
	i = i + 4

	binary.LittleEndian.PutUint16(b[i:], BINLOG_DUMP_NEVER_STOP)
	i = i + 2

	binary.LittleEndian.PutUint32(b[i:], followerID)
	i = i + 4

	copy(b[i:], p.Name)

	return b
}

func (f *Follower) writeRegisterFollowerCommand() error {
	f.c.resetSequence()

	data := makeRegisterFollowerCommand(f.Hostname(), f.port, f.user, f.password, f.followerID, f.masterID)

	return f.c.writePacket(data)
}

func makeRegisterFollowerCommand(hostname string, port uint16, user string, password string, followerID uint32, masterId uint32) []byte {
	b := make([]byte, 4+1+4+1+len(hostname)+1+len(user)+1+len(password)+2+4+4)
	i := 4

	b[i] = COM_REGISTER_SLAVE
	i++

	binary.LittleEndian.PutUint32(b[i:], followerID)
	i = i + 4

	// This should be the follower's hostname, not the name of the host we're connecting to.
	b[i] = uint8(len(hostname))
	i = i + 1

	n := copy(b[i:], hostname)
	i = i + n

	b[i] = uint8(len(user))
	i = i + 1

	n = copy(b[i:], user)
	i = i + n

	b[i] = uint8(len(password))
	i = i + 1
	n = copy(b[i:], password)
	i = i + n

	binary.LittleEndian.PutUint16(b[i:], port)
	i = i + 2

	// Replication rank (not used)
	binary.LittleEndian.PutUint32(b[i:], 0)
	i = i + 4

	binary.LittleEndian.PutUint32(b[i:], masterId)

	return b
}

func (f *Follower) replySemiSyncAck(p Position) error {
	f.c.resetSequence()

	data := makeSemiSyncAck(p)

	err := f.c.writePacket(data)
	if err != nil {
		return err
	}

	_, err = f.c.readOKPacket()
	if err != nil {
	}
	return err
}

func makeSemiSyncAck(p Position) []byte {
	b := make([]byte, 4+1+8+len(p.Name))
	pos := 4

	b[pos] = SemiSyncIndicator
	pos++

	binary.LittleEndian.PutUint64(b[pos:], uint64(p.Pos))
	pos += 8

	copy(b[pos:], p.Name)

	return b
}

// parseEventsTo(*Streamer) processes the raw binlog dump stream from the master
// one event at a time and sends them to the streamer.
func (f *Follower) parseEventsTo(str *Streamer) {
	defer f.wg.Done()

	// For each event, parse if OK; stop and close if unreadable.
	for {
		b, err := f.c.readPacket()
		if err != nil {
			str.closeWithError(err)
			return
		}

		switch b[0] {
		case OK_HEADER:
			if err = f.parseEvent(str, b); err != nil {
				str.closeWithError(err)
				return
			}
		case ERR_HEADER:
			err = f.c.handleErrorPacket(b)
			str.closeWithError(err)
			return
		default:
			str.closeWithError(fmt.Errorf("invalid stream header %c", b[0]))
			return
		}
	}
}

func (f *Follower) parseEvent(str *Streamer, b []byte) error {
	// The binlog network stream prepends every event with on OK byte; skip it.
	b = b[1:]

	needACK := false
	if f.semiSyncEnabled && (b[0] == SemiSyncIndicator) {
		needACK = (b[1] == 0x01)
		// Skip semi-sync header
		b = b[2:]
	}

	e, err := f.parser.Parse(b)
	if err != nil {
		return err
	}

	f.NextPosition.Pos = e.Header.LogPos

	if re, ok := e.Event.(*RotateEvent); ok {
		f.NextPosition.Name = string(re.NextFile)
		f.NextPosition.Pos = uint32(re.NextPosition)
	}

	needStop := false
	select {
	case str.ch <- e:
	case <-f.stopChan:
		needStop = true
	}

	if needACK {
		err := f.replySemiSyncAck(f.NextPosition)
		if err != nil {
			return err
		}
	}

	if needStop {
		return errors.New("sync stopping")
	}

	return nil
}

func (f *Follower) Close() {
	f.m.Lock()

	if f.c != nil {
		f.c.setReadDeadline(time.Now().Add(100 * time.Millisecond))
	}

	select {
	case f.stopChan <- struct{}{}:
	default:
	}

	f.wg.Wait()

	if f.c != nil {
		f.c.close()
	}

	f.running = false
	f.c = nil

	f.m.Unlock()
}
