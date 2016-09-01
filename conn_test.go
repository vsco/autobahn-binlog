package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeCommandWithArg(t *testing.T) {
	command := byte(0xFF)
	output := makeCommandWithArg(command, "hello")
	want := []byte{0, 0, 0, 0, command, 'h', 'e', 'l', 'l', 'o'}

	assert.Equal(t, output, want)
}

func TestEofPacketIsRecognizedAsEof(t *testing.T) {
	input := []byte{EOF_HEADER, 0}
	isEof := isEOFPacket(input)

	assert.True(t, isEof)
}

func TestNonEofPacketIsNotRecognizedAsEof(t *testing.T) {
	input1 := []byte{EOF_HEADER, 0, 1, 2, 3, 4, 5}
	isEof1 := isEOFPacket(input1)

	input2 := []byte{'i', 'm', 'u'}
	isEof2 := isEOFPacket(input2)

	assert.False(t, isEof1)
	assert.False(t, isEof2)
}
