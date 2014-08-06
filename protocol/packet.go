package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

var PacketSizeError = errors.New("kafka: Unsupported packet size.")

const maxPacketSize = 32 * 1024 * 1024 // 32MB

func ReadPacket(r io.Reader) (packet []byte, err error) {
	var b [4]byte
	_, err = io.ReadFull(r, b[:])
	if err != nil {
		return nil, err
	}
	length := int32(binary.BigEndian.Uint32(b[:]))
	if length < 0 || length > maxPacketSize {
		return nil, PacketSizeError
	}
	packet = make([]byte, int(length))
	n, err := io.ReadFull(r, packet)
	return packet[:n], err
}

func WritePacket(w io.Writer, packet []byte) (n int, err error) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(packet)))
	n, err = w.Write(b[:])
	if err != nil {
		return n, err
	}
	m, err := w.Write(packet)
	return n + m, err
}
