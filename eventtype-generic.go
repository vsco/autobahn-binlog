package binlog

import (
	"encoding/binary"
)

// In this file: events that are irrelevant to us.

// "Transaction ID for 2PC, written whenever a COMMIT is expected."
type XidEvent struct {
	Xid uint64
}

func NewXidEvent(b []byte) (Event, error) {
	e := new(XidEvent)

	// XID (8 bytes)
	e.Xid = binary.LittleEndian.Uint64(b)

	return e, nil
}

// The query event is used to send text queries the correct binlog.
type QueryEvent struct {
	SlaveProxyID  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
	DatabaseName  []byte
	Query         []byte
}

func NewQueryEvent(b []byte) (Event, error) {
	e := new(QueryEvent)
	i := 0

	// Slave proxy ID (4 bytes)
	e.SlaveProxyID = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// Execution time (4 bytes)
	e.ExecutionTime = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// Database name length (1 byte)
	dbNameLength := uint8(b[i])
	i = i + 1

	// Error code (2 bytes)
	e.ErrorCode = binary.LittleEndian.Uint16(b[i : i+2])
	i = i + 2

	// Status-vars length (2 bytes)
	statusVarsLength := binary.LittleEndian.Uint16(b[i : i+2])
	i = i + 2

	// Status-vars (string[$len])
	e.StatusVars = b[i : i+int(statusVarsLength)]
	i = i + int(statusVarsLength)

	// DatabaseName (string[$len])
	e.DatabaseName = b[i : i+int(dbNameLength)]
	i = i + int(dbNameLength)

	// Skip [00] byte
	i = i + 1

	// Query (string[EOF])
	e.Query = b[i:]

	return e, nil
}

// "truncate a file and set block-data"
type BeginLoadQueryEvent struct {
	FileID    uint32
	BlockData []byte
}

func NewBeginLoadQueryEvent(b []byte) (Event, error) {
	e := new(BeginLoadQueryEvent)

	i := 0

	// File ID (4)
	e.FileID = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// Block data (string[EOF])
	e.BlockData = b[i:]

	return e, nil
}

type ExecuteLoadQueryEvent struct {
	SlaveProxyID     uint32
	ExecutionTime    uint32
	SchemaLength     uint8
	ErrorCode        uint16
	StatusVars       uint16
	FileID           uint32
	StartPos         uint32
	EndPos           uint32
	DupHandlingFlags uint8
}

func NewExecuteLoadQueryEvent(b []byte) (Event, error) {
	e := new(ExecuteLoadQueryEvent)
	i := 0

	// Slave proxy ID (4 bytes)
	e.SlaveProxyID = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// Execution time (4 bytes)
	e.ExecutionTime = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// Schema length (1 byte)
	e.SchemaLength = uint8(b[i])
	i = i + 1

	// Error code (2 bytes)
	e.ErrorCode = binary.LittleEndian.Uint16(b[i : i+2])
	i = i + 2

	// Status-vars length (2 byte)
	e.StatusVars = binary.LittleEndian.Uint16(b[i : i+2])
	i = i + 2

	// File ID (4 bytes)
	e.FileID = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// Start position (4 nytes)
	e.StartPos = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// End position (4 bytes)
	e.EndPos = binary.LittleEndian.Uint32(b[i : i+4])
	i = i + 4

	// Dup handling flags (1 byte)
	e.DupHandlingFlags = uint8(b[i])

	return e, nil
}

// Catchall for all unnamed events
type GenericEvent struct {
	Data []byte
}

func NewGenericEvent(b []byte) (Event, error) {
	e := new(GenericEvent)
	e.Data = b
	return e, nil
}
