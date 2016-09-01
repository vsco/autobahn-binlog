package binlog

import (
	"errors"
	"fmt"
	"strconv"
)

type Result struct {
	Status       uint16
	InsertId     uint64
	AffectedRows uint64
	*Resultset
}

type Resultset struct {
	Fields     []*Field
	FieldNames map[string]int
	Values     [][]interface{}
	RowDatas   []RowData
}

type RowData []byte

func (r *Resultset) GetString(row, column int) (string, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return "", err
	}

	switch v := d.(type) {
	case string:
		return v, nil
	case []byte:
		return superUnsafeGetString(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case nil:
		return "", nil
	default:
		return "", fmt.Errorf("data type is %T", v)
	}
}

func (r *Resultset) GetInt64(row, column int) (int64, error) {
	v, err := r.GetUint64(row, column)
	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

func (r *Resultset) GetUint64(row, column int) (uint64, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return 0, err
	}

	switch v := d.(type) {
	case uint64:
		return v, nil
	case int64:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("data type is %T", v)
	}
}

func (r *Resultset) GetValue(row, column int) (interface{}, error) {
	if row >= len(r.Values) || row < 0 {
		return nil, fmt.Errorf("invalid row index %d", row)
	}

	if column >= len(r.Fields) || column < 0 {
		return nil, fmt.Errorf("invalid column index %d", column)
	}

	return r.Values[row][column], nil
}

func (p RowData) parse(f []*Field, binary bool) ([]interface{}, error) {
	if binary {
		return p.parseBinary(f)
	} else {
		return p.parseText(f)
	}
}

func (p RowData) parseText(fields []*Field) ([]interface{}, error) {
	b := make([]interface{}, len(fields))

	var (
		err    error
		v      []byte
		isNull bool
	)

	i := 0
	n := 0

	for fieldIdx := range fields {
		v, isNull, n, err = getLengthEncodedString(p[i:])
		if err != nil {
			return nil, err
		}

		i = i + n

		if isNull {
			b[fieldIdx] = nil
		} else {
			isUnsigned := fields[fieldIdx].Flag&UNSIGNED_FLAG != 0

			switch fields[fieldIdx].Type {
			case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24,
				MYSQL_TYPE_LONGLONG, MYSQL_TYPE_YEAR:
				if isUnsigned {
					b[fieldIdx], err = strconv.ParseUint(string(v), 10, 64)
				} else {
					b[fieldIdx], err = strconv.ParseInt(string(v), 10, 64)
				}
			case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
				b[fieldIdx], err = strconv.ParseFloat(string(v), 64)
			default:
				b[fieldIdx] = v
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return b, nil
}

func (p RowData) parseBinary(f []*Field) ([]interface{}, error) {
	b := make([]interface{}, len(f))

	if p[0] != OK_HEADER {
		return nil, errors.New("malformed packet error")
	}

	pos := 1 + ((len(f) + 7 + 2) >> 3)

	nullBitmap := p[1:pos]

	var isNull bool
	var n int
	var err error
	var v []byte
	for i := range b {
		if nullBitmap[(i+2)/8]&(1<<(uint(i+2)%8)) > 0 {
			b[i] = nil
			continue
		}

		isUnsigned := f[i].Flag&UNSIGNED_FLAG != 0

		switch f[i].Type {
		case MYSQL_TYPE_NULL:
			b[i] = nil
			continue

		case MYSQL_TYPE_TINY:
			if isUnsigned {
				b[i] = getBinaryUint8(p[pos : pos+1])
			} else {
				b[i] = getBinaryInt8(p[pos : pos+1])
			}
			pos++
			continue

		case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
			if isUnsigned {
				b[i] = getBinaryUint16(p[pos : pos+2])
			} else {
				b[i] = getBinaryInt16(p[pos : pos+2])
			}
			pos += 2
			continue

		case MYSQL_TYPE_INT24:
			if isUnsigned {
				b[i] = getBinaryUint24(p[pos : pos+3])
			} else {
				b[i] = getBinaryInt24(p[pos : pos+3])
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONG:
			if isUnsigned {
				b[i] = getBinaryUint32(p[pos : pos+4])
			} else {
				b[i] = getBinaryInt32(p[pos : pos+4])
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONGLONG:
			if isUnsigned {
				b[i] = getBinaryUint64(p[pos : pos+8])
			} else {
				b[i] = getBinaryInt64(p[pos : pos+8])
			}
			pos += 8
			continue

		case MYSQL_TYPE_FLOAT:
			b[i] = getBinaryFloat32(p[pos : pos+4])
			pos += 4
			continue

		case MYSQL_TYPE_DOUBLE:
			b[i] = getBinaryFloat64(p[pos : pos+4])
			pos += 8
			continue

		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING, MYSQL_TYPE_GEOMETRY:
			v, isNull, n, err = getLengthEncodedString(p[pos:])
			pos += n
			if err != nil {
				return nil, err
			}

			if !isNull {
				b[i] = v
				continue
			} else {
				b[i] = nil
				continue
			}
		case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
			var num uint64
			num, isNull, n = getLengthEncodedInt(p[pos:])

			pos = pos + n

			if isNull {
				b[i] = nil
				continue
			}

			b[i], err = formatBinaryDate(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
			var num uint64
			num, isNull, n = getLengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				b[i] = nil
				continue
			}

			b[i], err = formatBinaryDateTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIME:
			var num uint64
			num, isNull, n = getLengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				b[i] = nil
				continue
			}

			b[i], err = formatBinaryTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("Stmt unknown field type %d %s", f[i].Type, f[i].Name)
		}
	}

	return b, nil
}
