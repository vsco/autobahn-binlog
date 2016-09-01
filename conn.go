// Package binlog handles establishment of a follower connection to a MySQL server,
// streaming of binary log data, and parsing into memory.
package binlog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	timeout                 time.Duration = 10 * time.Second
	maxPayloadLength        int           = 1<<24 - 1
	initialPacketBufferSize int           = 4 * 1024 // 4 kb
)

// Conn speaks the MySQL client/server protocol documented here:
// https://dev.mysql.com/doc/internals/en/client-server-protocol.html.
type Conn struct {
	conn net.Conn
	br   *bufio.Reader
	seq  uint8 // packet sequence number

	user     string
	password string
	db       string

	// Handshake initialization packet from server
	capability   uint32
	status       uint16
	charset      string
	salt         []byte
	connectionID uint32
}

// NewConn opens a new connection to a MySQL server and returns it.
func NewConn(host string, port uint16, user string, password string, dbName string) (*Conn, error) {
	c := new(Conn)
	c.user = user
	c.password = password
	c.db = dbName
	c.charset = DEFAULT_CHARSET

	var err error
	address := fmt.Sprintf("%s:%d", host, port)
	c.conn, err = net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}

	c.br = bufio.NewReaderSize(c.conn, initialPacketBufferSize)

	if err = c.handshake(); err != nil {
		return nil, err
	}

	return c, nil
}

// handshake performs the handshake to establish the connection.
func (c *Conn) handshake() error {
	var err error
	if err = c.readInitialHandshake(); err != nil {
		c.close()
		return err
	}

	if err := c.writeAuthHandshake(); err != nil {
		c.close()

		return err
	}

	if _, err := c.readOKPacket(); err != nil {
		c.close()
		return err
	}

	return nil
}

// readInitialHandshake reads the handshake initialization packet.
func (c *Conn) readInitialHandshake() error {
	b, err := c.readPacket()
	if err != nil {
		return err
	}
	if b[0] == ERR_HEADER {
		return errors.New("initial handshake error")
	}
	if b[0] < MinProtocolVersion {
		return fmt.Errorf("invalid protocol version %d, must >= 10", b[0])
	}

	i := 0

	// Skip protocol version (1 byte)
	i = i + 1

	// Skip (null-terminated) MySQL version
	i = i + bytes.IndexByte(b[1:], 0x00) + 1

	// Connection ID (4 bytes)
	c.connectionID = uint32(binary.LittleEndian.Uint32(b[i : i+4]))
	i = i + 4

	// Salt (8 bytes)
	c.salt = []byte{}
	c.salt = append(c.salt, b[i:i+8]...)
	i = i + 8

	// Skip filler null byte
	i = i + 1

	// Capability flags (lower 2 bytes)
	c.capability = uint32(binary.LittleEndian.Uint16(b[i : i+2]))
	i = i + 2

	if len(b) > i {
		// Skip server charset
		i = i + 1

		// Status flags
		c.status = binary.LittleEndian.Uint16(b[i : i+2])
		i = i + 2

		// Capability flags (upper 2 bytes)
		c.capability = uint32(binary.LittleEndian.Uint16(b[i:i+2]))<<16 | c.capability
		i = i + 2

		// Skip auth data len or [00]
		i = i + 1

		// Skip reserved (all [00])
		i = i + 10

		// Rest of the salt
		c.salt = append(c.salt, b[i:i+12]...)
	}

	return nil
}

func (c *Conn) execute(query string) (*Result, error) {
	err := c.writeCommandWithArg(COM_QUERY, query)
	if err != nil {
		return nil, err
	}

	return c.readResult(false)
}

func (c *Conn) handleErrorPacket(data []byte) error {
	return errors.New("error packet")
}

func (c *Conn) readOKPacket() (*Result, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == OK_HEADER {
		return c.handleOKPacket(data)
	} else if data[0] == ERR_HEADER {
		return nil, c.handleErrorPacket(data)
	} else {
		return nil, errors.New("invalid ok packet")
	}
}

func (c *Conn) writeCommandWithArg(command byte, arg string) error {
	c.resetSequence()

	data := makeCommandWithArg(command, arg)

	return c.writePacket(data)
}

func makeCommandWithArg(command byte, s string) []byte {
	b := make([]byte, len(s)+5)

	b[4] = command
	copy(b[5:], s)

	return b
}

func (c *Conn) readUntilEOF() (err error) {
	var b []byte

	for {
		b, err = c.readPacket()

		if err != nil {
			return
		}

		if isEOFPacket(b) {
			return
		}
	}
}

func isEOFPacket(b []byte) bool {
	return b[0] == EOF_HEADER && len(b) <= 5
}

func (c *Conn) handleOKPacket(b []byte) (*Result, error) {
	var n int
	i := 1

	r := new(Result)

	r.AffectedRows, _, n = getLengthEncodedInt(b[i:])
	i = i + n

	r.InsertId, _, n = getLengthEncodedInt(b[i:])
	i = i + n

	if c.capability&CLIENT_PROTOCOL_41 > 0 {
		r.Status = binary.LittleEndian.Uint16(b[i:])
		c.status = r.Status
		i = i + 2

	} else if c.capability&CLIENT_TRANSACTIONS > 0 {
		r.Status = binary.LittleEndian.Uint16(b[i:])
		c.status = r.Status
		i = i + 2
	}

	// Skip info
	return r, nil
}

func (c *Conn) readResult(binary bool) (*Result, error) {
	b, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	if b[0] == OK_HEADER {
		return c.handleOKPacket(b)
	} else if b[0] == ERR_HEADER {
		return nil, c.handleErrorPacket(b)
	} else if b[0] == LocalInFile_HEADER {
		return nil, errors.New("malformed packet error")
	}

	return c.readResultset(b, binary)
}

func (c *Conn) readResultset(b []byte, binary bool) (*Result, error) {
	result := &Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 0,
		Resultset:    &Resultset{},
	}

	columnCount, _, n := getLengthEncodedInt(b)

	if n-len(b) != 0 {
		return nil, errors.New("malformed packet error")
	}

	result.Fields = make([]*Field, columnCount)
	result.FieldNames = make(map[string]int, columnCount)

	if err := c.readResultColumns(result); err != nil {
		return nil, err
	}

	if err := c.readResultRows(result, binary); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Conn) readResultColumns(result *Result) (err error) {
	var i int = 0
	var b []byte

	for {
		b, err = c.readPacket()
		if err != nil {
			return
		}

		if isEOFPacket(b) {
			if c.capability&CLIENT_PROTOCOL_41 > 0 {

				result.Status = binary.LittleEndian.Uint16(b[3:])
				c.status = result.Status
			}

			if i != len(result.Fields) {
				err = errors.New("malformed packet error")
			}

			return
		}

		result.Fields[i], err = FieldData(b).parse()
		if err != nil {
			return
		}

		result.FieldNames[superUnsafeGetString(result.Fields[i].Name)] = i

		i = i + 1
	}
}

func (c *Conn) readResultRows(result *Result, isBinary bool) (err error) {
	var b []byte

	for {
		b, err = c.readPacket()

		if err != nil {
			return
		}

		if isEOFPacket(b) {
			if c.capability&CLIENT_PROTOCOL_41 > 0 {

				result.Status = binary.LittleEndian.Uint16(b[3:])
				c.status = result.Status
			}

			break
		}

		result.RowDatas = append(result.RowDatas, b)
	}

	result.Values = make([][]interface{}, len(result.RowDatas))

	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].parse(result.Fields, isBinary)

		if err != nil {
			return err
		}
	}

	return nil
}

// writeAuthHandshake generates the handshake response packet.
func (c *Conn) writeAuthHandshake() error {
	// Adjust client capability flags based on server support
	capability := CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION |
		CLIENT_LONG_PASSWORD | CLIENT_TRANSACTIONS | CLIENT_LONG_FLAG
	capability &= c.capability

	// Length: capability (4) + max-packet size (4) + charset (1) + reserved all[0]
	// (23) + username
	packetLength := 4 + 4 + 1 + 23 + (len(c.user) + 1)

	// Calculate hash
	auth := scramble41(c.salt, []byte(c.password))
	packetLength = packetLength + 1 + len(auth)

	if len(c.db) > 0 {
		capability |= CLIENT_CONNECT_WITH_DB
		packetLength = packetLength + len(c.db) + 1
	}
	c.capability = capability

	i := 0
	b := make([]byte, packetLength+4)
	i = i + 4

	// Client capability flags [32 bit]
	binary.LittleEndian.PutUint32(b[i:i+4], capability)
	i = i + 4

	// Max packet size [32 bit] (none)
	binary.LittleEndian.PutUint32(b[i:i+4], 0)
	i = i + 4

	// Client charset [1 byte]; use default collation ID 33 here (utf-8)
	b[i] = byte(DEFAULT_COLLATION_ID)
	i = i + 1

	// Filler [23 bytes] (all 0x00)
	i = i + 23

	// User [null terminated string]
	if len(c.user) > 0 {
		i = i + copy(b[i:], c.user)
	}
	b[i] = 0x00
	i = i + 1

	// Auth [length encoded integer]
	b[i] = byte(len(auth))
	i = i + 1 + copy(b[i+1:], auth)

	// DB [null terminated string]
	if len(c.db) > 0 {
		i = i + copy(b[i:], c.db)
		b[i] = 0x00
	}

	return c.writePacket(b)
}

func (c *Conn) write(b []byte) (int, error) {
	return c.conn.Write(b)
}

func (c *Conn) setReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// readPacket() reads a raw packet from the MySQL connection. A raw packet is
// (1) a binary log event sent from a master to a follower <- what we care about
// (2) a single statement sent to the MySQL server, or
// (3) a single row sent to the client.
func (c *Conn) readPacket() ([]byte, error) {
	var buf bytes.Buffer

	if err := c.readPacketTo(&buf); err != nil {
		return nil, err
	} else {
		return buf.Bytes(), nil
	}
}

// readPacketTo reads the next available network packet into the writer provided
// and increments the packet sequence number.
func (c *Conn) readPacketTo(w io.Writer) error {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(c.br, header); err != nil {
		return err
	}

	payloadLength := int(getBinaryUint24(header[0:3]))
	if payloadLength < 1 {
		return fmt.Errorf("invalid payload length %d", payloadLength)
	}

	// Check for out-of-order packets
	sequence := uint8(header[3])
	if sequence != c.seq {
		return fmt.Errorf("invalid sequence %d != %d", sequence, c.seq)
	}

	c.seq = c.seq + 1

	// Now read the payload
	if n, err := io.CopyN(w, c.br, int64(payloadLength)); err != nil {
		return err
	} else if n != int64(payloadLength) {
		return errors.New("connection is faulty")
	} else {
		if payloadLength < maxPayloadLength {
			return nil
		}
		if err := c.readPacketTo(w); err != nil {
			return err
		}
	}

	return nil
}

// writePacket populates the passed packet buffer with a proper header in place
// and writes it to the network.
func (c *Conn) writePacket(data []byte) error {
	payloadLength := len(data) - 4

	for payloadLength >= maxPayloadLength {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff
		data[3] = c.seq

		if n, err := c.write(data[:4+maxPayloadLength]); err != nil {
			return errors.New("connection was bad")
		} else if n != (4 + maxPayloadLength) {
			return errors.New("connection was bad")
		} else {
			c.seq = c.seq + 1
			payloadLength = payloadLength - maxPayloadLength
			data = data[maxPayloadLength:]
		}
	}

	data[0] = byte(payloadLength)
	data[1] = byte(payloadLength >> 8)
	data[2] = byte(payloadLength >> 16)
	data[3] = c.seq

	if n, err := c.write(data); err != nil {
		return errors.New("connection was bad")
	} else if n != len(data) {
		return errors.New("connection was bad")
	} else {
		c.seq = c.seq + 1
		return nil
	}
}

// resetSequence resets the packet sequence number.
func (c *Conn) resetSequence() {
	c.seq = 0
}

func (c *Conn) close() error {
	c.seq = 0
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
