package binlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

type RowsEvent struct {
	Table         *TableMapEvent
	TableID       uint64
	Flags         uint16
	ColumnCount   uint64
	ColumnBitmap1 []byte          //len = (ColumnCount + 7) / 8
	ColumnBitmap2 []byte          //if UPDATE_ROWS_EVENT_V1 or v2, len = (ColumnCount + 7) / 8
	Rows          [][]interface{} //rows: invalid: int64, float64, bool, []byte, string
}

// Payload is structured as follows for MySQL v5.5:
//   19 bytes for common v4 event header
//   6 bytes (uint64) for table id
//   2 bytes (uint16) for flags
//   1 to 9 bytes (net_store_length variable encoded uint64), Z, for total
//     number of columns
//   ceil(Z / 8) bytes for bitmap indicating which columns are used (for update
//     events, this bitmap is used for the before image)
//   (v1/v2 update events specific) ceil(Z / 8) bytes for bitmap indicating which
//     columns are used in the after image
//   The remaining body contains the row data (row values are decoded based
//       on current table context):
//     v1/v2 write/delete events specific:
//       List of rows
//     v1/v2 update events specific:
//       List of pairs of (before image row, after image row)
//     Each row image is composed of:
//       bit field indicating whether each field in the row is NULL.
//       list of non-NULL encoded values.
func NewRowsEvent(tables map[uint64]*TableMapEvent, eventType EventType, b []byte) (Event, error) {
	e := new(RowsEvent)
	i := 0

	// Table ID (6 bytes): either 0x00ffffff (a dummy event) or a table defined by a
	// TableMapEvent
	const tableIDSize = 6
	e.TableID = getLittleEndianFixedLengthInt(b[i:tableIDSize])
	i = i + tableIDSize

	// Flags (2 bytes)
	e.Flags = binary.LittleEndian.Uint16(b[i : i+2])
	i = i + 2

	var n int
	e.ColumnCount, _, n = getLengthEncodedInt(b[i:])
	i = i + n

	bitCount := bitmapByteSize(int(e.ColumnCount))
	e.ColumnBitmap1 = b[i : i+bitCount]
	i = i + bitCount

	if eventType == UPDATE_ROWS_EVENT_V1 {
		e.ColumnBitmap2 = b[i : i+bitCount]
		i = i + bitCount
	}

	var ok bool
	e.Table, ok = tables[e.TableID]
	if !ok {
		return nil, errors.New("invalid table id")
	}

	// Repeatedly parse rows until end of event
	var err error
	for i < len(b) {
		if n, err = e.parseRows(b[i:], e.Table, e.ColumnBitmap1); err != nil {
			return nil, err
		}
		i = i + n

		if eventType == UPDATE_ROWS_EVENT_V1 {
			if n, err = e.parseRows(b[i:], e.Table, e.ColumnBitmap2); err != nil {
				return nil, err
			}
			i = i + n
		}
	}
	return e, nil
}

func (e *RowsEvent) parseRows(b []byte, table *TableMapEvent, bitmap []byte) (int, error) {
	row := make([]interface{}, e.ColumnCount)
	i := 0
	count := byteCountFromBitCount(bitCount(bitmap))

	nullBitmap := b[i : i+count]
	i = i + count
	nullBitIndex := 0

	var n int
	var err error
	for j := 0; j < int(e.ColumnCount); j++ {
		if getBit(bitmap, j) == 0 {
			continue
		}

		isNull := (uint32(nullBitmap[nullBitIndex/8]) >> uint32(nullBitIndex%8)) & 0x01

		if isNull > 0 {
			row[j] = nil
			nullBitIndex = nullBitIndex + 1
			continue
		}

		row[j], n, err = parseValue(b[i:], table.ColumnTypes[j], table.ColumnMetadata[j])

		if err != nil {
			return 0, nil
		}
		i = i + n
		nullBitIndex = nullBitIndex + 1
	}

	e.Rows = append(e.Rows, row)
	return i, nil
}

// Ref: MySQL sql/log_event.cc > log_event_print_value
func parseValue(data []byte, tp byte, meta uint16) (v interface{}, n int, err error) {
	var length int = 0

	if tp == MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			b1 := uint8(meta & 0xFF)

			if b0&0x30 != 0x30 {
				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
				tp = byte(b0 | 0x30)
			} else {
				length = int(meta & 0xFF)
				tp = b0
			}
		} else {
			length = int(meta)
		}
	}

	// The MySQL binary replication protocol doesn't tell us whether a field is a
	// signed or unsigned int; we have to process the value differently on the
	// receiving side, where we know more about the table schema.
	switch tp {
	case MYSQL_TYPE_NULL:
		return nil, 0, nil
	case MYSQL_TYPE_TINY:
		return getBinaryInt8(data), 1, nil
	case MYSQL_TYPE_SHORT:
		return getBinaryInt16(data), 2, nil
	case MYSQL_TYPE_INT24:
		return getBinaryInt24(data[0:3]), 3, nil
	case MYSQL_TYPE_LONG:
		return getBinaryInt32(data), 4, nil
	case MYSQL_TYPE_LONGLONG:
		return getBinaryInt64(data), 8, nil
	case MYSQL_TYPE_NEWDECIMAL:
		return parseDecimalType(data, meta)
	case MYSQL_TYPE_FLOAT:
		return getBinaryFloat32(data), 4, nil
	case MYSQL_TYPE_DOUBLE:
		return getBinaryFloat64(data), 8, nil
	case MYSQL_TYPE_BIT:
		return parseBitType(data, meta)
	case MYSQL_TYPE_TIMESTAMP:
		t := binary.LittleEndian.Uint32(data)
		return time.Unix(int64(t), 0), 4, nil
	case MYSQL_TYPE_TIMESTAMP2:
		return parseTimestamp2Type(data, meta)
	case MYSQL_TYPE_DATETIME:
		return parseDateTime(data)
	case MYSQL_TYPE_DATETIME2:
		return decodeDatetime2(data, meta)
	case MYSQL_TYPE_TIME:
		n = 3
		i32 := uint32(getLittleEndianFixedLengthInt(data[0:3]))
		if i32 == 0 {
			v = "00:00:00"
		} else {
			sign := ""
			if i32 < 0 {
				sign = "-"
			}
			v = fmt.Sprintf("%s%02d:%02d:%02d", sign, i32/10000, (i32%10000)/100, i32%100)
		}
		return v, n, nil
	case MYSQL_TYPE_TIME2:
		return parseTime2Type(data, meta)
	case MYSQL_TYPE_DATE:
		n = 3
		i32 := uint32(getLittleEndianFixedLengthInt(data[0:3]))
		if i32 == 0 {
			v = "0000-00-00"
		} else {
			v = fmt.Sprintf("%04d-%02d-%02d", i32/(16*32), i32/32%16, i32%32)
		}
		return v, n, err
	case MYSQL_TYPE_YEAR:
		return parseYear(data)
	case MYSQL_TYPE_ENUM:
		l := meta & 0xFF
		switch l {
		case 1:
			v = int64(data[0])
			n = 1
		case 2:
			v = int64(binary.BigEndian.Uint16(data))
			n = 2
		default:
			err = fmt.Errorf("Unknown ENUM packlen=%d", l)
		}
		return v, n, err
	case MYSQL_TYPE_SET:
		nbits := meta & 0xFF
		n = int(nbits+7) / 8
		v, err = parseBit(data, int(nbits), n)
		return v, n, err
	case MYSQL_TYPE_BLOB:
		switch meta {
		case 1:
			length = int(data[0])
			v = data[1 : 1+length]
			n = length + 1
		case 2:
			length = int(binary.LittleEndian.Uint16(data))
			v = data[2 : 2+length]
			n = length + 2
		case 3:
			length = int(getLittleEndianFixedLengthInt(data[0:3]))
			v = data[3 : 3+length]
			n = length + 3
		case 4:
			length = int(binary.LittleEndian.Uint32(data))
			v = data[4 : 4+length]
			n = length + 4
		default:
			err = fmt.Errorf("invalid blob packlen = %d", meta)
		}
		return v, n, err
	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING:
		return parseString(data, int(meta))
	case MYSQL_TYPE_STRING:
		return parseString(data, length)
	default:
		return v, n, fmt.Errorf("unsupported type %d in binlog", tp)
	}
}

func parseYear(b []byte) (string, int, error) {
	v := b[0]
	var str string

	if v == 0 {
		str = "0000"
	} else {
		str = strconv.Itoa(int(b[0]) + 1900)
	}

	return str, 1, nil
}

func parseBitType(b []byte, meta uint16) (int64, int, error) {
	numBits := int(((meta >> 8) * 8) + (meta & 0xFF))
	numBytes := byteCountFromBitCount(numBits)
	v, err := parseBit(b, numBits, numBytes)
	return v, numBytes, err
}

func parseBit(b []byte, numBits int, length int) (int64, error) {
	var (
		value int64
		err   error
	)
	if numBits > 1 {
		switch length {
		case 1:
			value = int64(b[0])
		case 2:
			value = int64(binary.BigEndian.Uint16(b))
		case 4:
			value = int64(binary.BigEndian.Uint32(b))
		case 3, 5, 6, 7:
			value = int64(getBigEndianFixedLengthInt(b[0:length]))
		case 8:
			value = int64(binary.BigEndian.Uint64(b))
		default:
			err = fmt.Errorf("invalid bit length %d", length)
		}
	} else {
		if length != 1 {
			err = fmt.Errorf("invalid bit length %d", length)
		} else {
			value = int64(b[0])
		}
	}
	return value, err
}

// Ref: https://github.com/jeremycole/mysql_binlog, vitess
func parseDecimalType(data []byte, meta uint16) (float64, int, error) {
	precision := int(meta >> 8)
	decimals := int(meta & 0xFF)
	integral := (precision - decimals)
	uncompIntegral := int(integral / digitsPerInteger)
	uncompFractional := int(decimals / digitsPerInteger)
	compIntegral := integral - (uncompIntegral * digitsPerInteger)
	compFractional := decimals - (uncompFractional * digitsPerInteger)

	binSize := uncompIntegral*4 + compressedBytes[compIntegral] +
		uncompFractional*4 + compressedBytes[compFractional]

	buf := make([]byte, binSize)
	copy(buf, data[:binSize])

	// Going to destroy data
	data = buf

	// Support negative decimals:
	// The sign is encoded in the high bit of the the byte, but this bit may also be used in the value
	value := uint32(data[0])
	var res bytes.Buffer
	var mask uint32 = 0
	if value&0x80 == 0 {
		mask = uint32((1 << 32) - 1)
		res.WriteString("-")
	}

	// Clear sign
	data[0] ^= 0x80

	pos, value := parseDecimalDecompressValue(compIntegral, data, uint8(mask))
	res.WriteString(fmt.Sprintf("%d", value))

	for i := 0; i < uncompIntegral; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos = pos + 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	res.WriteString(".")

	for i := 0; i < uncompFractional; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos = pos + 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	if size, value := parseDecimalDecompressValue(compFractional, data[pos:], uint8(mask)); size > 0 {
		res.WriteString(fmt.Sprintf("%0*d", compFractional, value))
		pos = pos + size
	}

	f, err := strconv.ParseFloat(getUnsafeString(res.Bytes()), 64)
	return f, pos, err
}

func parseDateTime(b []byte) (time.Time, int, error) {
	val := binary.LittleEndian.Uint64(b)
	d := val / 1000000
	t := val % 1000000
	v := time.Date(
		int(d/10000),              // year
		time.Month((d%10000)/100), // month
		int(d%100),                // day
		int(t/10000),              // hour
		int((t%10000)/100),        // minute
		int(t%100),                // second
		0,                         // nanosecond
		time.UTC)
	return v, 8, nil
}

func parseString(data []byte, length int) (v string, n int, err error) {
	if length < 256 {
		length = int(data[0])
		n = int(length) + 1
		v = getUnsafeString(data[1:n])
	} else {
		length = int(binary.LittleEndian.Uint16(data[0:]))
		n = length + 2
		v = getUnsafeString(data[2:n])
	}
	return v, n, nil
}

const digitsPerInteger int = 9

var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func parseDecimalDecompressValue(compIndx int, data []byte, mask uint8) (size int, value uint32) {
	size = compressedBytes[compIndx]
	databuff := make([]byte, size)
	for i := 0; i < size; i++ {
		databuff[i] = data[i] ^ mask
	}
	value = uint32(getBigEndianFixedLengthInt(databuff))
	return
}

func parseTimestamp2Type(data []byte, meta uint16) (string, int, error) {
	numBytes := int(4 + (meta+1)/2)
	sec := int64(binary.BigEndian.Uint32(data[0:4]))
	usec := int64(0)

	switch meta {
	case 1, 2:
		usec = int64(data[4]) * 10000
	case 3, 4:
		usec = int64(binary.BigEndian.Uint16(data[4:])) * 100
	case 5, 6:
		usec = int64(getBigEndianFixedLengthInt(data[4:7]))
	}

	if sec == 0 {
		return "0000-00-00 00:00:00", numBytes, nil
	}

	t := time.Unix(sec, usec*1000)
	return t.Format(TimeFormat), numBytes, nil
}

func decodeDatetime2(data []byte, meta uint16) (string, int, error) {
	n := int(5 + (meta+1)/2)

	intPart := int64(getBigEndianFixedLengthInt(data[0:5])) - DATETIMEF_INT_OFS
	var frac int64 = 0

	switch meta {
	case 1, 2:
		frac = int64(data[5]) * 10000
	case 3, 4:
		frac = int64(binary.BigEndian.Uint16(data[5:7])) * 100
	case 5, 6:
		frac = int64(getBigEndianFixedLengthInt(data[5:8]))
	}

	if intPart == 0 {
		return "0000-00-00 00:00:00", n, nil
	}

	tmp := intPart<<24 + frac

	if tmp < 0 {
		tmp = -tmp
	}

	// Ignore second part (precision)
	ymdhms := tmp >> 24

	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)

	day := int(ymd % (1 << 5))
	month := int(ym % 13)
	year := int(ym / 13)

	second := int(hms % (1 << 6))
	minute := int((hms >> 6) % (1 << 6))
	hour := int((hms >> 12))

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second), n, nil
}

const TIMEF_OFS int64 = 0x800000000000
const TIMEF_INT_OFS int64 = 0x800000

func parseTime2Type(data []byte, meta uint16) (string, int, error) {
	numBytes := int(3 + (meta+1)/2)

	tmp := int64(0)
	intPart := int64(0)
	frac := int64(0)

	switch meta {
	case 1:
	case 2:
		intPart = int64(getBigEndianFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(data[3])
		if intPart < 0 && frac > 0 {
			intPart = intPart + 1 // Shift to the next integer value
			frac = frac - 0x100   /* -(0x100 - frac) */
		}
		tmp = intPart<<24 + frac*10000
	case 3:
	case 4:
		intPart = int64(getBigEndianFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		frac = int64(binary.BigEndian.Uint16(data[3:5]))
		if intPart < 0 && frac > 0 {
			// Fix reverse fractional part order: "0x10000 - frac"
			intPart = intPart + 1 // Shift to the next integer value
			frac = frac - 0x10000
		}
		tmp = intPart<<24 + frac*100
	case 5:
	case 6:
		tmp = int64(getBigEndianFixedLengthInt(data[0:6])) - TIMEF_OFS
	default:
		intPart = int64(getBigEndianFixedLengthInt(data[0:3])) - TIMEF_INT_OFS
		tmp = intPart << 24
	}

	if intPart == 0 {
		return "00:00:00", numBytes, nil
	}

	hms := int64(0)
	sign := ""
	if tmp < 0 {
		tmp = -tmp
		sign = "-"
	}

	// Ignore second part (precision)
	hms = tmp >> 24

	hour := (hms >> 12) % (1 << 10)
	minute := (hms >> 6) % (1 << 6)
	second := hms % (1 << 6)

	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second), numBytes, nil
}

func getUnsafeString(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return s
}
