package binlog

import (
	"encoding/binary"
	"errors"
	"io"
)

// A TableMapEvent defines the structure of tables that are about to be changed.
// We don't support old_row_based_repl_4_byte_map_id_master mode.
type TableMapEvent struct {
	TableID        uint64
	Flags          uint16
	DatabaseName   []byte
	TableName      []byte
	ColumnCount    uint64
	ColumnTypes    []byte
	ColumnMetadata []uint16
	NullBitVector  []byte
	// coldescriptors?
}

// Payload is structured as follows for MySQL v5.5:
//   19 bytes for common v4 event header
//   6 bytes (uint64) for table id
//   2 bytes (uint16) for flags
//   1 byte (uint8), x, for database name length
//   x + 1 bytes for db name (zero-terminated)
//   1 byte (uint8), y, for table name length
//   y + 1 bytes for table name (zero-terminated)
//   1 to 9 bytes (net_store_length variable encoded uint64), z, for number of
//     columns
//   z bytes for column types (1 byte per column)
//   1 to 9 bytes (net_store_length variable encoded uint64), w, for field
//     metadata size
//   w bytes for field metadata
//   ceil(z / 8) bytes for nullable columns (1 bit per column)
func NewTableMapEvent(format *FormatDescriptionEvent, b []byte) (Event, error) {
	var tableIDSize int
	if format.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] == 6 {
		tableIDSize = 4
	} else {
		tableIDSize = 6
	}

	e := new(TableMapEvent)
	i := 0

	// Numeric table ID (now 6 bytes, previously 4)
	e.TableID = getLittleEndianFixedLengthInt(b[0:tableIDSize])
	i = i + tableIDSize

	// Flags (2 bytes)
	e.Flags = binary.LittleEndian.Uint16(b[i:])
	i = i + 2

	// Length of the database name (1 byte)
	dbNameLength := int(b[i])
	i = i + 1

	// Database name (string[dbNameLength])
	dbName := b[i : i+dbNameLength+1]
	i = i + dbNameLength + 1
	e.DatabaseName = dbName[:dbNameLength] // drop the null-termination char

	// Length of the table name
	tblNameLength := int(b[i])
	i = i + 1

	// Table name (string[tableLength])
	tblName := b[i : i+tblNameLength+1]
	i = i + tblNameLength + 1
	e.TableName = tblName[:tblNameLength] // drop the null-termination char

	// Number of columns in the table map (lenenc-int)
	var n int
	e.ColumnCount, _, n = getLengthEncodedInt(b[i:])
	i = i + n

	// Array of column definitions: one byte per field type
	e.ColumnTypes = b[i : i+int(e.ColumnCount)]
	i = i + int(e.ColumnCount)

	// Array of metadata per column (lenenc-str):
	// length is the overall length of the metadata array in bytes
	// length of each metadata field is dependent on the column's field type
	var err error
	var metadata []byte
	if metadata, _, n, err = getLengthEncodedString(b[i:]); err != nil {
		return nil, err
	}
	if err = e.parseMetadata(metadata); err != nil {
		return nil, err
	}
	i = i + n

	// A bitmask containing a bit set for each column that can be null.
	if len(b[i:]) != bitmapByteSize(int(e.ColumnCount)) {
		return nil, io.EOF
	}
	e.NullBitVector = b[i:]

	return e, nil
}

func (e *TableMapEvent) parseMetadata(b []byte) error {
	e.ColumnMetadata = make([]uint16, e.ColumnCount)
	i := 0

	for col, t := range e.ColumnTypes {
		switch t {
		// Tightly packed due to MySQL Bug #37426 ref: https://bugs.mysql.com/bug.php?id=37426
		case MYSQL_TYPE_STRING:
			x := uint16(b[i]) << 8 // type
			x = x + uint16(b[i+1]) // length
			e.ColumnMetadata[col] = x
			i = i + 2
		case MYSQL_TYPE_VAR_STRING,
			MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT:
			e.ColumnMetadata[col] = binary.LittleEndian.Uint16(b[i:])
			i = i + 2
		case MYSQL_TYPE_NEWDECIMAL:
			x := uint16(b[i]) << 8 // precision
			x = x + uint16(b[i+1]) // decimals
			e.ColumnMetadata[col] = x
			i = i + 2
		case MYSQL_TYPE_BLOB,
			MYSQL_TYPE_DOUBLE,
			MYSQL_TYPE_FLOAT,
			MYSQL_TYPE_GEOMETRY:
			e.ColumnMetadata[col] = uint16(b[i])
			i = i + 1
		case MYSQL_TYPE_TIME2,
			MYSQL_TYPE_DATETIME2,
			MYSQL_TYPE_TIMESTAMP2:
			e.ColumnMetadata[col] = uint16(b[i])
			i = i + 1
		// These shouldn't appear, give up
		case MYSQL_TYPE_NEWDATE,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_SET,
			MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB,
			MYSQL_TYPE_LONG_BLOB:
			return errors.New("unsupported type")
		default:
			e.ColumnMetadata[col] = 0
		}
	}

	return nil
}

// Note: MySQL docs claim this is (n+8)/7, but the below is actually correct
func bitmapByteSize(columnCount int) int {
	return int(columnCount+7) / 8
}
