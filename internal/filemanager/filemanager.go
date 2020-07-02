package filemanager

import (
	"os"
	"sync"
)

// FileManager FileManager
type FileManager interface {
	Iter(func(int64, *os.File) error) error
	Add(id int64, file *os.File)
	Get(id int64) *os.File
	Close()
}

type fileManager struct {
	sync.Mutex
	m map[int64]*os.File
}

// New New
func New() FileManager {
	return &fileManager{}
}

func (fm *fileManager) Iter(iter func(int64, *os.File) error) error {
	for id, file := range fm.m {
		if err := iter(id, file); err != nil {
			return err
		}
	}
	return nil
}

func (fm *fileManager) Add(id int64, file *os.File) {
	fm.m[id] = file
}

func (fm *fileManager) Get(id int64) *os.File {
	return fm.m[id]
}

func (fm *fileManager) Close() {
	for _, file := range fm.m {
		file.Close()
	}
}
