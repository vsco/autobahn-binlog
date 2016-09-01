package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type countTest struct {
	inData  int
	wantVal int
}

var byteFromBitTests = []countTest{
	{14, 2},
	{0, 0},
	{3, 1},
}

func TestByteCountFromBitCount(t *testing.T) {
	for _, bt := range byteFromBitTests {
		output := byteCountFromBitCount(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

type binaryTest struct {
	inData  []byte
	wantVal interface{}
}

var binaryInt8Tests = []binaryTest{
	{[]byte{255}, int8(-1)},
	{[]byte{128}, int8(-128)},
	{[]byte{127}, int8(127)},
	{[]byte{0}, int8(0)},
}

func TestGetBinaryInt8(t *testing.T) {
	for _, bt := range binaryInt8Tests {
		output := getBinaryInt8(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

func TestGetBinaryUint8(t *testing.T) {
	input := []byte{255}
	want := uint8(255)
	output := getBinaryUint8(input)

	assert.Equal(t, output, want)
}

var binaryInt16Tests = []binaryTest{
	{[]byte{254, 255}, int16(-2)},
	{[]byte{2, 1}, int16(258)},
	{[]byte{0, 0}, int16(0)},
}

func TestGetBinaryInt16(t *testing.T) {
	for _, bt := range binaryInt16Tests {
		output := getBinaryInt16(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

var binaryUint16Tests = []binaryTest{
	{[]byte{1, 2}, uint16(513)},
	{[]byte{255, 254}, uint16(65279)},
	{[]byte{0, 0}, uint16(0)},
}

func TestGetBinaryUint16(t *testing.T) {
	for _, bt := range binaryUint16Tests {
		output := getBinaryUint16(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

var binaryInt24Tests = []binaryTest{
	{[]byte{1, 2, 3}, int32(197121)},
	{[]byte{255, 254, 253}, int32(-131329)},
	{[]byte{0, 0, 0}, int32(0)},
}

func TestGetBinaryInt24(t *testing.T) {
	for _, bt := range binaryInt24Tests {
		output := getBinaryInt24(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

var binaryUint24Tests = []binaryTest{
	{[]byte{1, 2, 3}, uint32(197121)},
	{[]byte{255, 254, 253}, uint32(16645887)},
	{[]byte{0, 0, 0}, uint32(0)},
}

func TestGetBinaryUint24(t *testing.T) {
	for _, bt := range binaryUint24Tests {
		output := getBinaryUint24(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

var binaryInt32Tests = []binaryTest{
	{[]byte{1, 2, 3, 4}, int32(67305985)},
	{[]byte{255, 254, 253, 252}, int32(-50462977)},
	{[]byte{0, 0, 0, 0}, int32(0)},
}

func TestGetBinaryInt32(t *testing.T) {
	for _, bt := range binaryInt32Tests {
		output := getBinaryInt32(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

var binaryUint32Tests = []binaryTest{
	{[]byte{1, 2, 3, 4}, uint32(67305985)},
	{[]byte{255, 254, 253, 252}, uint32(4244504319)},
	{[]byte{0, 0, 0, 0}, uint32(0)},
}

func TestGetBinaryUint32(t *testing.T) {
	for _, bt := range binaryUint32Tests {
		output := getBinaryUint32(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

var binaryInt64Tests = []binaryTest{
	{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, int64(578437695752307201)},
	{[]byte{255, 254, 253, 252, 251, 250, 249, 248}, int64(-506097522914230529)},
	{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, int64(0)},
}

func TestGetBinaryInt64(t *testing.T) {
	for _, bt := range binaryInt64Tests {
		output := getBinaryInt64(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

var binaryUint64Tests = []binaryTest{
	{[]byte{1, 2, 3, 4, 0, 0, 0, 0}, uint64(67305985)},
	{[]byte{255, 254, 253, 252, 0, 0, 0, 0}, uint64(4244504319)},
	{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, uint64(0)},
}

func TestGetBinaryUint64(t *testing.T) {
	for _, bt := range binaryUint64Tests {
		output := getBinaryUint64(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

var binaryFloat32Tests = []binaryTest{
	{[]byte{0, 100, 0, 200}, float32(-131472)},
	{[]byte{20, 101, 0, 201}, float32(-525905.25)},
	{[]byte{20, 101, 0, 73}, float32(525905.25)},
	{[]byte{0, 0, 0, 0}, float32(0)},
}

func TestGetBinaryFloat32(t *testing.T) {
	for _, bt := range binaryFloat32Tests {
		output := getBinaryFloat32(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

// var binaryFloat64Tests = []binaryTest{
// 	{[]byte{0, 0, 0, 0, 0, 100, 0, 200}, float64(-131472)},
// 	{[]byte{0, 0, 0, 0, 20, 101, 0, 201}, float64(-525905.25)},
// 	{[]byte{0, 0, 0, 0, 20, 101, 0, 73}, float64(525905.25)},
// 	{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, float64(0)},
// }

// func TestGetBinaryFloat64(t *testing.T) {
// 	for _, bt := range binaryFloat64Tests {
// 		output := getBinaryFloat64(bt.inData)
// 		assert.Equal(t, output, bt.wantVal)
// 	}
// }

var binaryLittleEndianFixedLengthIntTests = []binaryTest{
	{[]byte{0, 0, 0}, uint64(0)},
}

func TestGetLittleEndianFixedLengthInt(t *testing.T) {
	for _, bt := range binaryLittleEndianFixedLengthIntTests {
		output := getLittleEndianFixedLengthInt(bt.inData)
		assert.Equal(t, output, bt.wantVal)
	}
}

// // little-endian
// func getLittleEndianFixedLengthInt(buf []byte) uint64 {
// 	var num uint64 = 0
// 	for i, b := range buf {
// 		num |= uint64(b) << (uint(i) * 8)
// 	}
// 	return num
// }

// // big-endian
// func getBigEndianFixedLengthInt(buf []byte) uint64 {
// 	var num uint64 = 0
// 	for i, b := range buf {
// 		num |= uint64(b) << (uint(len(buf)-i-1) * 8)
// 	}
// 	return num
// }

// // getLengthEncodedInt reads from the passed-in buffer and returns the number as
// // stored in length-encoded integer format or true if it's null, plus the number
// // of bytes read.
// func getLengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
// 	switch b[0] {

// 	// 251: NULL
// 	case 0xfb:
// 		n = 1
// 		isNull = true
// 		return

// 	// 252: value of following 2
// 	case 0xfc:
// 		num = uint64(b[1]) | uint64(b[2])<<8
// 		n = 3
// 		return

// 	// 253: value of following 3
// 	case 0xfd:
// 		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
// 		n = 4
// 		return

// 	// 254: value of following 8
// 	case 0xfe:
// 		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
// 			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
// 			uint64(b[7])<<48 | uint64(b[8])<<56
// 		n = 9
// 		return
// 	}

// 	// 0-250: value of first byte
// 	num = uint64(b[0])
// 	n = 1
// 	return
// }

// func putLengthEncodedInt(n uint64) []byte {
// 	switch {
// 	case n <= 250:
// 		return []byte{byte(n)}

// 	case n <= 0xffff:
// 		return []byte{0xfc, byte(n), byte(n >> 8)}

// 	case n <= 0xffffff:
// 		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}

// 	case n <= 0xffffffffffffffff:
// 		return []byte{0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
// 			byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56)}
// 	}
// 	return nil
// }

// func getLengthEncodedString(b []byte) ([]byte, bool, int, error) {
// 	// Get length
// 	num, isNull, n := getLengthEncodedInt(b)
// 	if num < 1 {
// 		return nil, isNull, n, nil
// 	}

// 	n = n + int(num)

// 	// Check data length
// 	if len(b) >= n {
// 		return b[n-int(num) : n], false, n, nil
// 	}
// 	return nil, false, n, io.EOF
// }

// func skipLengthEncodedString(b []byte) (int, error) {
// 	// Get length
// 	num, _, n := getLengthEncodedInt(b)
// 	if num < 1 {
// 		return n, nil
// 	}

// 	n = n + int(num)

// 	// Check data length
// 	if len(b) >= n {
// 		return n, nil
// 	}
// 	return n, io.EOF
// }

var putBinaryUint16Tests = []struct {
	in   uint16
	want []byte
}{
	{uint16(0), []byte{0, 0}},
	{uint16(10), []byte{10, 0}},
	{uint16(256), []byte{0, 1}},
	{uint16(65535), []byte{0xFF, 0xFF}},
}

func TestPutBinaryUint16(t *testing.T) {
	for _, bt := range putBinaryUint16Tests {
		output := putBinaryUint16(bt.in)
		assert.Equal(t, output, bt.want)
	}
}

var putBinaryUint32Tests = []struct {
	in   uint32
	want []byte
}{
	{uint32(0), []byte{0, 0, 0, 0}},
	{uint32(10), []byte{10, 0, 0, 0}},
	{uint32(256), []byte{0, 1, 0, 0}},
	{uint32(4294967295), []byte{0xFF, 0xFF, 0xFF, 0xFF}},
}

func TestPutBinaryUint32(t *testing.T) {
	for _, bt := range putBinaryUint32Tests {
		output := putBinaryUint32(bt.in)
		assert.Equal(t, output, bt.want)
	}
}

var putBinaryUint64Tests = []struct {
	in   uint64
	want []byte
}{
	{uint64(0), []byte{0, 0, 0, 0, 0, 0, 0, 0}},
}

func TestPutBinaryUint64(t *testing.T) {
	for _, bt := range putBinaryUint64Tests {
		output := putBinaryUint64(bt.in)
		assert.Equal(t, output, bt.want)
	}
}

func TestScramble41ScramblesWell(t *testing.T) {
	scramble := []byte{1, 2, 3, 4}
	password := []byte{'i', 'm', 'a', 'p', 'w', 'd'}
	want := []byte{0xeb, 0x6a, 0x59, 0xfd, 0x26, 0x17, 0xe3, 0x84, 0x36, 0x8b, 0xd1, 0xb7, 0x6a, 0xd, 0x68, 0xdb, 0x27, 0x22, 0x75, 0x3}
	output := scramble41(scramble, password)

	assert.Equal(t, output, want)
}

func TestSuperUnsafeGetNormalString(t *testing.T) {
	input := []byte("1234")
	want := "1234"
	output := superUnsafeGetString(input)

	assert.Equal(t, output, want)
}

func TestSuperUnsafeGetEmptyString(t *testing.T) {
	input := []byte("")
	want := ""
	output := superUnsafeGetString(input)

	assert.Equal(t, output, want)
}

func TestSuperUnsafeGetNilString(t *testing.T) {
	var input []byte = nil
	want := ""
	output := superUnsafeGetString(input)

	assert.Equal(t, output, want)
}
