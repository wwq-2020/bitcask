package codec

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/wwq1988/bitcask/internal/entry"
)

const (
	headerSize = 4
	crcSize    = 4
)

// Factory Factory
type Factory func(rw ReadWriterWithReadAt) Codec

// Codec Codec
type Codec interface {
	Decode() (*entry.Entry, error)
	DecodeAt(int64) (*entry.Entry, error)
	Encode(*entry.Entry) error
}

// ReadWriterWithReadAt ReadWriterWithReadAt
type ReadWriterWithReadAt interface {
	io.ReadWriter
	io.ReaderAt
}

type codec struct {
	rw ReadWriterWithReadAt
}

// New New
func New(rw ReadWriterWithReadAt) Codec {
	return &codec{rw: rw}
}

func (c *codec) Decode() (*entry.Entry, error) {
	buf := make([]byte, headerSize)
	if _, err := c.rw.Read(buf); err != nil {
		return nil, err
	}
	if len(buf) < headerSize {
		return nil, errors.New("data crupt")
	}
	keySize := binary.BigEndian.Uint16(buf[:2])
	valueSize := binary.BigEndian.Uint16(buf[2:])
	totalSize := keySize + valueSize + crcSize
	buf = make([]byte, totalSize)
	if _, err := c.rw.Read(buf); err != nil {
		return nil, err
	}
	if len(buf) < int(totalSize) {
		return nil, errors.New("data crupt")
	}
	key := buf[:keySize]
	value := buf[keySize : keySize+valueSize]
	entry := entry.NewEntry(key, value)
	return entry, nil
}

func (c *codec) DecodeAt(offset int64) (*entry.Entry, error) {
	buf := make([]byte, headerSize)
	if _, err := c.rw.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	if len(buf) < headerSize {
		return nil, errors.New("data crupt")
	}
	keySize := binary.BigEndian.Uint16(buf[:2])
	valueSize := binary.BigEndian.Uint16(buf[2:])
	totalSize := keySize + valueSize + crcSize
	buf = make([]byte, totalSize)
	if _, err := c.rw.ReadAt(buf, offset+headerSize); err != nil {
		return nil, err
	}
	if len(buf) < int(totalSize) {
		return nil, errors.New("data crupt")
	}
	key := buf[:keySize]
	value := buf[keySize : keySize+valueSize]
	entry := entry.NewEntry(key, value)
	return entry, nil
}

func (c *codec) Encode(entry *entry.Entry) error {
	keySize := len(entry.Key)
	valueSize := len(entry.Value)
	buf := make([]byte, 0, keySize+valueSize+crcSize)
	crcBytes := make([]byte, crcSize)
	binary.BigEndian.PutUint32(crcBytes, entry.CRC)
	copy(buf, entry.Key)
	copy(buf[keySize:], entry.Value)
	copy(buf[keySize+valueSize:], crcBytes)
	if _, err := c.rw.Write(buf); err != nil {
		return err
	}
	return nil
}
