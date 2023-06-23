// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/encoding/encoding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package binaryreader

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/dennwc/varint"
	"github.com/pkg/errors"

	"github.com/grafana/loki/pkg/util"
)

var (
	ErrInvalidSize     = errors.New("invalid size")
	ErrInvalidChecksum = errors.New("invalid checksum")
)

// Decbuf provides safe methods to extract data from a binary file. It does all
// necessary bounds checking and advancing of the binary file. Several datums can
// be extracted without checking for errors. However, before using any datum, the
// Err() method must be checked. New file-backed Decbuf instances must be created
// via DecbufFactory
type Decbuf struct {
	r *fileReader
	E error
}

func (d *Decbuf) Uvarint() int      { return int(d.Uvarint64()) }
func (d *Decbuf) Be32int() int      { return int(d.Be32()) }
func (d *Decbuf) Uvarint32() uint32 { return uint32(d.Uvarint64()) }
func (d *Decbuf) Be64int64() int64  { return int64(d.Be64()) }

// CheckCrc32 checks the integrity of the contents of this Decbuf,
// comparing the contents with the CRC32 checksum stored in the last four bytes.
// CheckCrc32 consumes the contents of this Decbuf.
func (d *Decbuf) CheckCrc32(castagnoliTable *crc32.Table) (actual uint32, expected uint32) {
	if d.r.len() <= 4 {
		d.E = ErrInvalidSize
		return 0, 0
	}

	hash := crc32.New(castagnoliTable)
	bytesToRead := d.r.len() - 4
	maxChunkSize := 1024 * 1024
	rawBuf := make([]byte, maxChunkSize)

	for bytesToRead > 0 {
		chunkSize := util.Min(bytesToRead, maxChunkSize)
		chunkBuf := rawBuf[0:chunkSize]

		err := d.r.readInto(chunkBuf)
		if err != nil {
			d.E = errors.Wrap(err, "read contents for CRC32 calculation")
			return 0, 0
		}

		if n, err := hash.Write(chunkBuf); err != nil {
			d.E = errors.Wrap(err, "write bytes to CRC32 calculation")
			return 0, 0
		} else if n != len(chunkBuf) {
			d.E = fmt.Errorf("CRC32 calculation only wrote %v bytes, expected to write %v bytes", n, len(chunkBuf))
			return 0, 0
		}

		bytesToRead -= len(chunkBuf)
	}

	actual = hash.Sum32()
	expected = d.Be32()

	if actual != expected {
		d.E = ErrInvalidChecksum
	}

	return actual, expected
}

// Skip advances the pointer of the underlying fileReader by the given number
// of bytes. If E is non-nil, this method has no effect. Skip-ing beyond the
// end of the underlying fileReader will set E to an error and not advance the
// pointer of the fileReader.
func (d *Decbuf) Skip(l int) {
	if d.E != nil {
		return
	}

	d.E = d.r.skip(l)
}

// SkipUvarintBytes advances the pointer of the underlying fileReader past the
// next varint-prefixed bytes. If E is non-nil, this method has no effect.
func (d *Decbuf) SkipUvarintBytes() {
	l := d.Uvarint64()
	d.Skip(int(l))
}

// ResetAt sets the pointer of the underlying fileReader to the absolute
// offset and discards any buffered data. If E is non-nil, this method has
// no effect. ResetAt-ing beyond the end of the underlying fileReader will set
// E to an error and not advance the pointer of fileReader.
func (d *Decbuf) ResetAt(off int) {
	if d.E != nil {
		return
	}

	d.E = d.r.resetAt(off)
}

// UvarintStr reads varint prefixed bytes into a string and consumes them. The string
// returned allocates its own memory may be used after subsequent reads from the Decbuf.
// If E is non-nil, this method returns an empty string.
func (d *Decbuf) UvarintStr() string {
	return string(d.UnsafeUvarintBytes())
}

// UnsafeUvarintBytes reads varint prefixed bytes into a byte slice consuming them but without
// allocating. The bytes returned are NO LONGER VALID after subsequent reads from the Decbuf.
// If E is non-nil, this method returns an empty byte slice.
func (d *Decbuf) UnsafeUvarintBytes() []byte {
	l := d.Uvarint64()
	if d.E != nil {
		return nil
	}

	// If the length of this uvarint slice is greater than the size of buffer used
	// by our file reader, we can't peek() it. Instead, we have to use the read() method
	// which will allocate its own slice to hold the results. We prefer to use peek()
	// when possible for performance but can't rely on slices always being less than
	// the size of our buffer.
	if l > uint64(d.r.size()) {
		b, err := d.r.read(int(l))
		if err != nil {
			d.E = err
			return nil
		}

		return b
	}

	b, err := d.r.peek(int(l))
	if err != nil {
		d.E = err
		return nil
	}

	if len(b) != int(l) {
		d.E = ErrInvalidSize
		return nil
	}

	if b == nil {
		return nil
	}

	err = d.r.skip(len(b))
	if err != nil {
		d.E = err
		return nil
	}

	return b
}

func (d *Decbuf) Uvarint64() uint64 {
	if d.E != nil {
		return 0
	}
	b, err := d.r.peek(10)
	if err != nil {
		d.E = err
		return 0
	}

	x, n := varint.Uvarint(b)
	if n < 1 {
		d.E = ErrInvalidSize
		return 0
	}

	err = d.r.skip(n)
	if err != nil {
		d.E = err
		return 0
	}

	return x
}

func (d *Decbuf) Be64() uint64 {
	if d.E != nil {
		return 0
	}

	b, err := d.r.peek(8)
	if err != nil {
		d.E = err
		return 0
	}

	if len(b) != 8 {
		d.E = ErrInvalidSize
		return 0
	}

	v := binary.BigEndian.Uint64(b)
	err = d.r.skip(8)
	if err != nil {
		d.E = err
		return 0
	}

	return v
}

func (d *Decbuf) Be32() uint32 {
	if d.E != nil {
		return 0
	}

	b, err := d.r.peek(4)
	if err != nil {
		d.E = err
		return 0
	}

	if len(b) != 4 {
		d.E = ErrInvalidSize
		return 0
	}

	v := binary.BigEndian.Uint32(b)
	err = d.r.skip(4)
	if err != nil {
		d.E = err
		return 0
	}

	return v
}

func (d *Decbuf) Byte() byte {
	if d.E != nil {
		return 0
	}

	b, err := d.r.peek(1)
	if err != nil {
		d.E = err
		return 0
	}

	if len(b) != 1 {
		d.E = ErrInvalidSize
		return 0
	}

	v := b[0]
	err = d.r.skip(1)
	if err != nil {
		d.E = err
		return 0
	}

	return v
}

func (d *Decbuf) Err() error { return d.E }

// Len returns the remaining number of bytes in the underlying fileReader.
func (d *Decbuf) Len() int { return d.r.len() }

// Get returns the remaining bytes in the underlying fileReader.
func (d *Decbuf) Get() []byte {
	v, err := d.r.read(d.Len() - 4) // last 4 bytes for checksum
	if err != nil {
		d.E = err
		return nil
	}
	return v
}

// Position returns the current position of the underlying fileReader.
// Calling d.ResetAt(d.Position()) is effectively a no-op.
func (d *Decbuf) Position() int { return d.r.position() }

func (d *Decbuf) Close() error {
	if d.r != nil {
		return d.r.close()
	}

	return nil
}