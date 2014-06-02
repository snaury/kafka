package protocol

import (
	"reflect"
	"testing"
)

func TestInsufficientData(t *testing.T) {
	test := func(maxsize int, tester func(PacketDecoder) error) {
		for size := 0; size < maxsize; size++ {
			d := NewBytesDecoder(make([]byte, size))
			err := tester(d)
			if err != InsufficientData {
				t.Errorf("Size %v/%v: expected InsufficientData, got %v", size, maxsize, err)
			}
		}
		d := NewBytesDecoder(make([]byte, maxsize))
		err := tester(d)
		if err != nil {
			t.Errorf("Size %v: expected no error, got %v", maxsize, err)
		}
	}
	test(1, func(d PacketDecoder) error { _, err := d.GetInt8(); return err })
	test(2, func(d PacketDecoder) error { _, err := d.GetInt16(); return err })
	test(4, func(d PacketDecoder) error { _, err := d.GetInt32(); return err })
	test(8, func(d PacketDecoder) error { _, err := d.GetInt64(); return err })
	test(4, func(d PacketDecoder) error { _, err := d.GetBytes(); return err })
	test(2, func(d PacketDecoder) error { _, err := d.GetString(); return err })
	test(2, func(d PacketDecoder) error { _, err := d.GetNullString(); return err })
	test(4, func(d PacketDecoder) error { _, err := d.GetInt32Array(); return err })
	test(4, func(d PacketDecoder) error { _, err := d.GetInt64Array(); return err })
}

func TestDecoding(t *testing.T) {
	report := func(err error) {
		if err != nil {
			t.Errorf("Unexpected error: %s (expected no error)", err)
		}
	}
	check := func(a, b interface{}, err error) {
		if err != nil {
			t.Errorf("Unexpected error: %s (expected %#v)", err, b)
			return
		}
		if !reflect.DeepEqual(a, b) {
			t.Errorf("Got %#v (expected %#v)", a, b)
		}
	}
	test := func(data []byte, tester func(PacketDecoder)) {
		d := NewBytesDecoder(data)
		tester(d)
		rest := d.Rest()
		if len(rest) != 0 {
			t.Errorf("Unread data found: {% x}", rest)
		}
	}
	checkInt8 := func(d PacketDecoder, e int8) { v, err := d.GetInt8(); check(v, e, err) }
	checkInt16 := func(d PacketDecoder, e int16) { v, err := d.GetInt16(); check(v, e, err) }
	checkInt32 := func(d PacketDecoder, e int32) { v, err := d.GetInt32(); check(v, e, err) }
	checkInt64 := func(d PacketDecoder, e int64) { v, err := d.GetInt64(); check(v, e, err) }
	checkBytes := func(d PacketDecoder, e []byte) { v, err := d.GetBytes(); check(v, e, err) }
	checkString := func(d PacketDecoder, e string) { v, err := d.GetString(); check(v, e, err) }
	checkNullString := func(d PacketDecoder, e NullString) { v, err := d.GetNullString(); check(v, e, err) }
	checkInt32Array := func(d PacketDecoder, e []int32) { v, err := d.GetInt32Array(); check(v, e, err) }
	checkInt64Array := func(d PacketDecoder, e []int64) { v, err := d.GetInt64Array(); check(v, e, err) }

	test(
		[]byte{
			0x01,
			0xff,
			0x80,
		},
		func(d PacketDecoder) {
			checkInt8(d, 1)
			checkInt8(d, -1)
			checkInt8(d, -0x80)
		})

	test(
		[]byte{
			0x00, 0x01,
			0xff, 0xff,
			0x80, 0x00,
		},
		func(d PacketDecoder) {
			checkInt16(d, 1)
			checkInt16(d, -1)
			checkInt16(d, -0x8000)
		})

	test(
		[]byte{
			0x00, 0x00, 0x00, 0x01,
			0xff, 0xff, 0xff, 0xff,
			0x80, 0x00, 0x00, 0x00,
		},
		func(d PacketDecoder) {
			checkInt32(d, 1)
			checkInt32(d, -1)
			checkInt32(d, -0x80000000)
		})

	test(
		[]byte{
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		},
		func(d PacketDecoder) {
			checkInt64(d, 1)
			checkInt64(d, -1)
			checkInt64(d, -0x8000000000000000)
		})

	test(
		[]byte{
			0xff, 0xff, 0xff, 0xff,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x05,
			0x01, 0x02, 0x03, 0x04, 0x05,
		},
		func(d PacketDecoder) {
			checkBytes(d, nil)
			checkBytes(d, []byte{})
			checkBytes(d, []byte{1, 2, 3, 4, 5})
		})

	test(
		[]byte{
			0x00, 0x05,
			'h', 'e', 'l', 'l', 'o',
			0xff, 0xff,
			0x00, 0x05,
			'h', 'e', 'l', 'l', 'o',
		},
		func(d PacketDecoder) {
			checkString(d, "hello")
			checkNullString(d, NullString{})
			checkNullString(d, NullString{"hello", true})
		})

	test(
		[]byte{
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x05,
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x02,
			0x00, 0x00, 0x00, 0x03,
			0x00, 0x00, 0x00, 0x04,
			0x00, 0x00, 0x00, 0x05,
		},
		func(d PacketDecoder) {
			checkInt32Array(d, nil)
			checkInt32Array(d, []int32{1, 2, 3, 4, 5})
		})

	test(
		[]byte{
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x05,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
		},
		func(d PacketDecoder) {
			checkInt64Array(d, nil)
			checkInt64Array(d, []int64{1, 2, 3, 4, 5})
		})

	test(
		[]byte{
			0x00, 0x00, 0x00, 0x0b,
			0xd8, 0xd2, 0xd6, 0x10,
			0x01,
			0x00, 0x02,
			0x00, 0x00, 0x00, 0x03,
		},
		func(d PacketDecoder) {
			d.Push(LengthField{})
			d.Push(CRC32Field{})
			checkInt8(d, 1)
			checkInt16(d, 2)
			checkInt32(d, 3)
			report(d.Pop())
			report(d.Pop())
		})
}
