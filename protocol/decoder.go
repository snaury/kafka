package protocol

import (
	"encoding/binary"
	"errors"
)

type DecodingError string

func (err DecodingError) Error() string {
	return "kafka: Error while decoding packet: " + string(err)
}

var InsufficientData = errors.New("kafka: Insufficient data while decoding packet.")

type PacketDecoder interface {
	GetInt8() (int8, error)
	GetInt16() (int16, error)
	GetInt32() (int32, error)
	GetInt64() (int64, error)
	GetRaw(n int) ([]byte, error)

	GetBytes() ([]byte, error)
	GetString() (string, error)
	GetNullString() (NullString, error)
	GetInt32Array() ([]int32, error)
	GetInt64Array() ([]int64, error)

	Push(in DeferredDecoder) error
	Pop() error
}

type DeferredDecoder interface {
	Length() int
	Decode(buf []byte) error
}

type Decoder interface {
	Decode(pd PacketDecoder) error
}

type pushedDecoder struct {
	off     int
	decoder DeferredDecoder
}

type BytesDecoder struct {
	buf   []byte
	off   int
	stack []pushedDecoder
}

func NewBytesDecoder(buf []byte) *BytesDecoder {
	return &BytesDecoder{
		buf: buf,
	}
}

func (d *BytesDecoder) get(n int) ([]byte, error) {
	if n < 0 || d.off+n > len(d.buf) {
		d.off = len(d.buf)
		return nil, InsufficientData
	}
	b := d.buf[d.off : d.off+n]
	d.off += n
	return b, nil
}

func (d *BytesDecoder) Rest() []byte {
	return d.buf[d.off:]
}

func (d *BytesDecoder) GetInt8() (int8, error) {
	b, err := d.get(1)
	if err != nil {
		return 0, err
	}
	return int8(b[0]), nil
}

func (d *BytesDecoder) GetInt16() (int16, error) {
	b, err := d.get(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func (d *BytesDecoder) GetInt32() (int32, error) {
	b, err := d.get(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func (d *BytesDecoder) GetInt64() (int64, error) {
	b, err := d.get(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func (d *BytesDecoder) GetRaw(n int) ([]byte, error) {
	return d.get(n)
}

func (d *BytesDecoder) GetBytes() ([]byte, error) {
	n, err := d.GetInt32()
	if err != nil {
		return nil, err
	}

	switch {
	case n < -1:
		return nil, DecodingError("invalid bytes size")
	case n == -1:
		return nil, nil
	case n == 0:
		return make([]byte, 0), nil
	}

	b, err := d.get(int(n))
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (d *BytesDecoder) GetString() (string, error) {
	n, err := d.GetInt16()
	if err != nil {
		return "", err
	}

	switch {
	case n < -1:
		return "", DecodingError("invalid string size")
	case n == -1:
		return "", nil
	case n == 0:
		return "", nil
	}

	b, err := d.get(int(n))
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func (d *BytesDecoder) GetNullString() (NullString, error) {
	n, err := d.GetInt16()
	if err != nil {
		return NullString{}, err
	}

	switch {
	case n < -1:
		return NullString{}, DecodingError("invalid string size")
	case n == -1:
		return NullString{}, nil
	case n == 0:
		return NullString{"", true}, nil
	}

	b, err := d.get(int(n))
	if err != nil {
		return NullString{}, err
	}

	return NullString{string(b), true}, nil
}

func (d *BytesDecoder) GetInt32Array() ([]int32, error) {
	n, err := d.GetInt32()
	if err != nil {
		return nil, err
	}

	switch {
	case n < 0 || n >= 536870912:
		return nil, DecodingError("invalid array size")
	case n == 0:
		return nil, nil
	}

	b, err := d.get(int(n) * 4)
	if err != nil {
		return nil, err
	}

	a := make([]int32, int(n))
	for i := range a {
		a[i] = int32(binary.BigEndian.Uint32(b[i*4:]))
	}

	return a, nil
}

func (d *BytesDecoder) GetInt64Array() ([]int64, error) {
	n, err := d.GetInt32()
	if err != nil {
		return nil, err
	}

	switch {
	case n < 0 || n >= 268435456:
		return nil, DecodingError("invalid array size")
	case n == 0:
		return nil, nil
	}

	b, err := d.get(int(n) * 8)
	if err != nil {
		return nil, err
	}

	a := make([]int64, int(n))
	for i := range a {
		a[i] = int64(binary.BigEndian.Uint64(b[i*8:]))
	}

	return a, nil
}

func (d *BytesDecoder) Push(in DeferredDecoder) error {
	off := d.off
	_, err := d.get(in.Length())
	if err != nil {
		return err
	}
	d.stack = append(d.stack, pushedDecoder{off, in})
	return nil
}

func (d *BytesDecoder) Pop() error {
	v := d.stack[len(d.stack)-1]
	d.stack = d.stack[:len(d.stack)-1]
	buf := d.buf[v.off:d.off]
	return v.decoder.Decode(buf)
}
