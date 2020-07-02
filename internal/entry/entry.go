package entry

import "hash/crc32"

// Entry Entry
type Entry struct {
	CRC    uint32
	Key    []byte
	Offset int64
	Value  []byte
}

// NewEntry NewEntry
func NewEntry(key, value []byte) *Entry {
	crc := crc32.ChecksumIEEE(value)
	return &Entry{
		CRC:   crc,
		Key:   key,
		Value: value,
	}
}
