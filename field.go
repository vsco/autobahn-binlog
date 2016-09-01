package binlog

import (
	"encoding/binary"
	"errors"
)

type FieldData []byte

type Field struct {
	Data               FieldData
	Schema             []byte
	Table              []byte
	OrgTable           []byte
	Name               []byte
	OrgName            []byte
	Charset            uint16
	ColumnLength       uint32
	Type               uint8
	Flag               uint16
	Decimal            uint8
	DefaultValueLength uint64
	DefaultValue       []byte
}

func (p FieldData) parse() (f *Field, err error) {
	f = new(Field)

	f.Data = p

	var n int
	i := 0

	// Skip catalog (always def)
	n, err = skipLengthEncodedString(p)
	if err != nil {
		return
	}
	i = i + n

	// Schema
	f.Schema, _, n, err = getLengthEncodedString(p[i:])
	if err != nil {
		return
	}
	i = i + n

	// Table
	f.Table, _, n, err = getLengthEncodedString(p[i:])
	if err != nil {
		return
	}
	i = i + n

	// Org table
	f.OrgTable, _, n, err = getLengthEncodedString(p[i:])
	if err != nil {
		return
	}
	i = i + n

	// Name
	f.Name, _, n, err = getLengthEncodedString(p[i:])
	if err != nil {
		return
	}
	i = i + n

	// Org name
	f.OrgName, _, n, err = getLengthEncodedString(p[i:])
	if err != nil {
		return
	}
	i = i + n

	// Skip oc
	i = i + 1

	// Charset
	f.Charset = binary.LittleEndian.Uint16(p[i:])
	i = i + 2

	// Column length
	f.ColumnLength = binary.LittleEndian.Uint32(p[i:])
	i = i + 4

	// Type
	f.Type = p[i]
	i = i + 1

	// Flag
	f.Flag = binary.LittleEndian.Uint16(p[i:])
	i = i + 2

	// Decimals
	f.Decimal = p[i]
	i = i + 1

	// Filter [0x00][0x00]
	i = i + 2

	f.DefaultValue = nil

	// If more data, command was field list
	if len(p) > i {

		// Length of default value (lenenc-int)
		f.DefaultValueLength, _, n = getLengthEncodedInt(p[i:])
		i = i + n

		if i+int(f.DefaultValueLength) > len(p) {
			err = errors.New("malformed packet")
			return
		}

		// Default value (string[len])
		f.DefaultValue = p[i:(i + int(f.DefaultValueLength))]
	}

	return
}
