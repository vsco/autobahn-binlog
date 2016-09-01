package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMysqlNullIsParsedProperly(t *testing.T) {
	v, n, err := parseValue([]byte{}, MYSQL_TYPE_NULL, 0)

	assert.Equal(t, v, nil)
	assert.Equal(t, n, 0)
	assert.NoError(t, err)
}

func TestMysqlTinyIntIsParsedProperly(t *testing.T) {
	v, n, err := parseValue([]byte{127}, MYSQL_TYPE_TINY, 0)

	assert.Equal(t, v, int8(127))
	assert.Equal(t, n, 1)
	assert.NoError(t, err)
}

func TestMysqlSmallIntIsParsedProperly(t *testing.T) {
	v, n, err := parseValue([]byte{255, 5}, MYSQL_TYPE_SHORT, 0)

	assert.Equal(t, v, int16(1535))
	assert.Equal(t, n, 2)
	assert.NoError(t, err)
}

func TestMysqlMediumIntIsParsedProperly(t *testing.T) {
	v, n, err := parseValue([]byte{255, 100, 1}, MYSQL_TYPE_INT24, 0)

	assert.Equal(t, v, int32(91391))
	assert.Equal(t, n, 3)
	assert.NoError(t, err)
}

func TestMysqlIntIsParsedProperly(t *testing.T) {
	v, n, err := parseValue([]byte{4, 3, 2, 1}, MYSQL_TYPE_LONG, 0)

	assert.Equal(t, v, int32(0x01020304))
	assert.Equal(t, n, 4)
	assert.NoError(t, err)
}

func TestMysqlBigIntIsParsedProperly(t *testing.T) {
	v, n, err := parseValue([]byte{255, 100, 1, 120, 255, 255, 255, 255}, MYSQL_TYPE_LONGLONG, 0)

	assert.Equal(t, v, int64(-2281609985))
	assert.Equal(t, n, 8)
	assert.NoError(t, err)
}

type parseTest struct {
	inData  []byte
	inMeta  uint16
	wantVal interface{}
	wantLen int
}

func metaFromPrecAndDec(precision int, decimals int) uint16 {
	return uint16(precision*256 + decimals)
}

var decimalTests = []parseTest{
	{[]byte{28, 156, 127, 241}, metaFromPrecAndDec(4, 2), float64(-99.99), 2},
	{[]byte{127, 241, 140, 113, 140}, metaFromPrecAndDec(5, 0), float64(-3699), 3},
	{[]byte{113, 140, 255, 245, 127, 255}, metaFromPrecAndDec(7, 3), float64(-3699.010), 4},
	{[]byte{127, 255, 241, 140, 254, 127, 255}, metaFromPrecAndDec(10, 2), float64(-3699.01), 5},
	{[]byte{127, 255, 241, 140, 255, 245, 127, 255}, metaFromPrecAndDec(10, 3), float64(-3699.010), 6},
	{[]byte{127, 255, 255, 241, 140, 254, 118, 196}, metaFromPrecAndDec(13, 2), float64(-3699.01), 6},
	{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, metaFromPrecAndDec(15, 14), float64(-9.99999999999999), 8},
	{[]byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 127, 255}, metaFromPrecAndDec(20, 10), float64(-3699.0100000000), 10},
	{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 241, 140, 255, 252, 23, 127, 255}, metaFromPrecAndDec(30, 5), float64(-3699.01000), 15},
	{[]byte{127, 255, 255, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 127, 241}, metaFromPrecAndDec(30, 20), float64(-3699.01000000000000000000), 14},
	{[]byte{127, 241, 140, 255, 103, 105, 127, 255, 255, 255, 255, 255, 255, 255, 255, 13, 0}, metaFromPrecAndDec(30, 25), float64(-3699.0100000000000000000000000), 15},
	{[]byte{128, 0, 0, 128, 0}, metaFromPrecAndDec(5, 0), float64(0), 3},
	{[]byte{117, 200, 127, 255}, metaFromPrecAndDec(4, 2), float64(-10.55), 2},
	{[]byte{127, 255, 244, 127, 245}, metaFromPrecAndDec(5, 0), float64(-11), 3},
	{[]byte{127, 245, 253, 217, 127, 255}, metaFromPrecAndDec(7, 3), float64(-10.550), 4},
	{[]byte{128, 1, 128, 0}, metaFromPrecAndDec(4, 2), float64(0.01), 2},
	{[]byte{128, 0, 0, 12, 128, 0}, metaFromPrecAndDec(7, 3), float64(0.012), 4},
	{[]byte{128, 0, 0, 0, 1, 128, 0}, metaFromPrecAndDec(10, 2), float64(0.01), 5},
	{[]byte{128, 0, 0, 0, 0, 12, 128, 0}, metaFromPrecAndDec(10, 3), float64(0.012), 6},
	{[]byte{128, 0, 0, 0, 0, 1, 128, 0}, metaFromPrecAndDec(13, 2), float64(0.01), 6},
	{[]byte{127, 255, 255, 245, 200, 127, 255}, metaFromPrecAndDec(10, 2), float64(-10.55), 5},
	{[]byte{127, 255, 255, 245, 253, 217, 127, 255}, metaFromPrecAndDec(10, 3), float64(-10.550), 6},
	{[]byte{127, 255, 255, 255, 245, 200, 118, 196}, metaFromPrecAndDec(13, 2), float64(-10.55), 6},
	{[]byte{118, 196, 101, 54, 0, 254, 121, 96, 127, 255}, metaFromPrecAndDec(15, 14), float64(-9.99999999999999), 8},
	{[]byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 127, 255}, metaFromPrecAndDec(20, 10), float64(-10.5500000000), 10},
	{[]byte{127, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 245, 255, 41, 39, 127, 255}, metaFromPrecAndDec(30, 5), float64(-10.55000), 15},
	{[]byte{127, 255, 255, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 127, 255}, metaFromPrecAndDec(30, 20), float64(-10.55000000000000000000), 14},
	{[]byte{127, 255, 245, 223, 55, 170, 127, 255, 255, 255, 255, 255, 255, 255, 255, 4, 0}, metaFromPrecAndDec(30, 25), float64(-10.5500000000000000000000000), 15},
}

func TestMysqlDecimalIsParsedProperly(t *testing.T) {
	meta := 1026
	precision := int(meta >> 8)
	decimals := int(meta & 0xFF)
	assert.Equal(t, precision, 4)
	assert.Equal(t, decimals, 2)

	for _, dt := range decimalTests {
		v, n, err := parseValue(dt.inData, MYSQL_TYPE_NEWDECIMAL, dt.inMeta)

		assert.Equal(t, dt.wantVal, v)
		assert.Equal(t, dt.wantLen, n)
		assert.NoError(t, err)
	}

	v, n, err := parseValue([]byte{3, 2, 1}, MYSQL_TYPE_NEWDECIMAL, 1025)

	assert.Equal(t, v, float64(-31997.254))
	assert.Equal(t, n, 3)
	assert.NoError(t, err)
}

func TestMysqlFloatIsParsedProperly(t *testing.T) {
	v, n, err := parseValue([]byte{0, 0, 0, 192}, MYSQL_TYPE_FLOAT, 0)

	assert.Equal(t, v, float32(-2))
	assert.Equal(t, n, 4)
	assert.NoError(t, err)
}

func TestMysqlDoubleIsParsedProperly(t *testing.T) {
	v, n, err := parseValue([]byte{0, 0, 0, 0, 0, 0, 0, 192}, MYSQL_TYPE_DOUBLE, 0)

	assert.Equal(t, float64(-2), v)
	assert.Equal(t, 8, n)
	assert.NoError(t, err)
}

var bitTests = []parseTest{
	{[]byte{7, 6, 5, 4, 3, 2, 1}, 55, int64(1976943448883713), 7},
	{[]byte{6, 5, 4, 3, 2, 1}, 42, int64(6618611909121), 6},
	{[]byte{5, 4, 3, 2, 1}, 35, int64(21542142465), 5},
	{[]byte{4, 3, 2, 1}, 30, int64(67305985), 4},
	{[]byte{255, 254, 253}, 17, int64(16776957), 3},
	{[]byte{255, 254}, 12, int64(65534), 2},
	{[]byte{9}, 7, int64(9), 1},
}

func TestMysqlBitIsParsedProperly(t *testing.T) {
	for _, bt := range bitTests {
		v, n, err := parseValue(bt.inData, MYSQL_TYPE_BIT, bt.inMeta)

		assert.Equal(t, bt.wantVal, v)
		assert.Equal(t, bt.wantLen, n)
		assert.NoError(t, err)
	}
}

var yearTests = []parseTest{
	{[]byte{14}, 0, "1914", 1},
	{[]byte{89}, 0, "1989", 1},
	{[]byte{116}, 0, "2016", 1},
}

func TestMysqlYearIsParsedProperly(t *testing.T) {
	for _, yt := range yearTests {
		v, n, err := parseValue(yt.inData, MYSQL_TYPE_YEAR, yt.inMeta)

		assert.Equal(t, yt.wantVal, v)
		assert.Equal(t, yt.wantLen, n)
		assert.NoError(t, err)
	}
}

var blobTests = []parseTest{
	{[]byte{5, 'h', 'e', 'l', 'l', 'o'}, 1, "hello", 6},
}

func TestMysqlBlobIsParsedProperly(t *testing.T) {
	for _, bt := range blobTests {
		v, n, err := parseValue(bt.inData, MYSQL_TYPE_VARCHAR, bt.inMeta)

		assert.Equal(t, bt.wantVal, v)
		assert.Equal(t, bt.wantLen, n)
		assert.NoError(t, err)
	}
}

var varcharTests = []parseTest{
	{[]byte{5, 'h', 'e', 'l', 'l', 'o'}, 5, "hello", 6},
	{[]byte{0}, 0, "", 1},
	{[]byte{10, 'h', 'e', 'l', 'l', 'o', 't', 'h', 'e', 'r', 'e'}, 10, "hellothere", 11},
}

func TestMysqlVarcharIsParsedProperly(t *testing.T) {
	for _, vt := range varcharTests {
		v, n, err := parseValue(vt.inData, MYSQL_TYPE_VARCHAR, vt.inMeta)

		assert.Equal(t, vt.wantVal, v)
		assert.Equal(t, vt.wantLen, n)
		assert.NoError(t, err)
	}
}

var stringTests = []parseTest{
	{[]byte{5, 'h', 'e', 'l', 'l', 'o'}, 5, "hello", 6},
	{[]byte{4, '<', 'h', 'r', '>'}, 4, "<hr>", 5},
	{[]byte{0}, 0, "", 1},
}

func TestMysqlStringIsParsedProperly(t *testing.T) {
	for _, st := range stringTests {
		v, n, err := parseValue(st.inData, MYSQL_TYPE_STRING, st.inMeta)

		assert.Equal(t, st.wantVal, v)
		assert.Equal(t, st.wantLen, n)
		assert.NoError(t, err)
	}
}

func TestGetUnsafeString(t *testing.T) {
	input := []byte{'a', 'b', 'c', 'd', 'e', 'f'}

	assert.Equal(t, getUnsafeString(input), "abcdef")
}
