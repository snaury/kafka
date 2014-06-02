package protocol

import (
	"encoding/binary"
	"errors"
)

var EncodingError = errors.New("kafka: Error while encoding packet.")

type PacketEncoder interface {
	PutInt8(in int8)
	PutInt16(in int16)
	PutInt32(in int32)
	PutInt64(in int64)
	PutRaw(in []byte)

	PutBytes(in []byte) error
	PutString(in string) error
	PutNullString(in NullString) error
	PutInt32Array(in []int32) error
	PutInt64Array(in []int64) error

	Push(in DeferredEncoder)
	Pop() error
}

type DeferredEncoder interface {
	Length() int
	Encode(buf []byte) error
}

type pushedEncoder struct {
	off     int
	encoder DeferredEncoder
}

type BytesEncoder struct {
	buf   []byte
	stack []pushedEncoder
}

func NewBytesEncoder() *BytesEncoder {
	return &BytesEncoder{}
}

func (e *BytesEncoder) grow(n int) []byte {
	if e.buf == nil {
		if n < 64 {
			// first time allocate at least 64 bytes
			e.buf = make([]byte, 64)[:n]
		} else {
			// otherwise allocate exactly n bytes
			e.buf = make([]byte, n)
		}
		return e.buf
	}
	m := len(e.buf)
	if m+n > cap(e.buf) {
		buf := make([]byte, 2*cap(e.buf)+n)
		copy(buf, e.buf)
		e.buf = buf[0 : m+n]
	} else {
		e.buf = e.buf[0 : m+n]
	}
	return e.buf[m : m+n]
}

func (e *BytesEncoder) Bytes() []byte {
	return e.buf
}

func (e *BytesEncoder) PutInt8(in int8) {
	b := e.grow(1)
	b[0] = byte(in)
}

func (e *BytesEncoder) PutInt16(in int16) {
	b := e.grow(2)
	binary.BigEndian.PutUint16(b, uint16(in))
}

func (e *BytesEncoder) PutInt32(in int32) {
	b := e.grow(4)
	binary.BigEndian.PutUint32(b, uint32(in))
}

func (e *BytesEncoder) PutInt64(in int64) {
	b := e.grow(8)
	binary.BigEndian.PutUint64(b, uint64(in))
}

func (e *BytesEncoder) PutRaw(in []byte) {
	if len(in) > 0 {
		b := e.grow(len(in))
		copy(b, in)
	}
}

func (e *BytesEncoder) PutBytes(in []byte) error {
	if in == nil {
		e.PutInt32(-1)
		return nil
	}
	if len(in) > 2147483647 {
		return EncodingError
	}
	e.PutInt32(int32(len(in)))
	e.PutRaw(in)
	return nil
}

func (e *BytesEncoder) PutString(in string) error {
	if len(in) > 32767 {
		return EncodingError
	}
	e.PutInt16(int16(len(in)))
	b := e.grow(len(in))
	copy(b, in)
	return nil
}

func (e *BytesEncoder) PutNullString(in NullString) error {
	if !in.Valid {
		e.PutInt16(-1)
		return nil
	}
	return e.PutString(in.String)
}

func (e *BytesEncoder) PutInt32Array(in []int32) error {
	if len(in) > 2147483647 {
		return EncodingError
	}
	e.PutInt32(int32(len(in)))
	b := e.grow(len(in) * 4)
	for i, v := range in {
		binary.BigEndian.PutUint32(b[i*4:], uint32(v))
	}
	return nil
}

func (e *BytesEncoder) PutInt64Array(in []int64) error {
	if len(in) > 2147483647 {
		return EncodingError
	}
	e.PutInt32(int32(len(in)))
	b := e.grow(len(in) * 8)
	for i, v := range in {
		binary.BigEndian.PutUint64(b[i*8:], uint64(v))
	}
	return nil
}

func (e *BytesEncoder) Push(in DeferredEncoder) {
	off := len(e.buf)
	e.grow(in.Length())
	e.stack = append(e.stack, pushedEncoder{off, in})
}

func (e *BytesEncoder) Pop() error {
	v := e.stack[len(e.stack)-1]
	e.stack = e.stack[:len(e.stack)-1]
	buf := e.buf[v.off:]
	return v.encoder.Encode(buf)
}
