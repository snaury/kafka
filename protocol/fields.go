package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type CRC32Field struct{}

func (c CRC32Field) Length() int {
	return 4
}

func (c CRC32Field) Encode(buf []byte) error {
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf, crc)
	return nil
}

func (c CRC32Field) Decode(buf []byte) error {
	crc := crc32.ChecksumIEEE(buf[4:])
	if crc != binary.BigEndian.Uint32(buf) {
		return DecodingError(fmt.Sprintf("CRC mismatch: 0x%08x != 0x%08x", crc, binary.BigEndian.Uint32(buf)))
	}
	return nil
}

type LengthField struct{}

func (l LengthField) Length() int {
	return 4
}

func (l LengthField) Encode(buf []byte) error {
	binary.BigEndian.PutUint32(buf, uint32(len(buf)-4))
	return nil
}

func (l LengthField) Decode(buf []byte) error {
	if uint32(len(buf)-4) != binary.BigEndian.Uint32(buf) {
		return DecodingError(fmt.Sprintf("Length mismatch: %d != %d", uint32(len(buf)-4), binary.BigEndian.Uint32(buf)))
	}
	return nil
}
