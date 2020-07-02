package codecmanager

import "github.com/wwq1988/bitcask/internal/codec"

// CodecManager CodecManager
type CodecManager interface {
	Get(int64) codec.Codec
	Add(int64, codec.Codec)
}

type codecManager struct {
	m map[int64]codec.Codec
}

// New New
func New() CodecManager {
	return &codecManager{}
}

func (cm *codecManager) Get(id int64) codec.Codec {
	return cm.m[id]
}

func (cm *codecManager) Add(id int64, codec codec.Codec) {
	cm.m[id] = codec
}
