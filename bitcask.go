package bitcask

import (
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
	"github.com/wwq1988/bitcask/internal/codecmanager"
	"github.com/wwq1988/bitcask/internal/entry"
	"github.com/wwq1988/bitcask/internal/filemanager"
	"github.com/wwq1988/bitcask/internal/index"
)

// DB DB
type DB struct {
	options      *Options
	flock        *flock.Flock
	index        index.Index
	lock         sync.Mutex
	curID        int64
	path         string
	fileManager  filemanager.FileManager
	codecManager codecmanager.CodecManager
}

// Open Open
func Open(path string, opts ...Option) (*DB, error) {
	if err := ensureDir(path); err != nil {
		return nil, err
	}
	options := genOptions(opts...)
	flock := flock.NewFlock(filepath.Join(path, "db.lock"))

	if err := flock.Lock(); err != nil {
		return nil, err
	}

	filenames, err := getFilenames(path)
	if err != nil {
		return nil, err
	}
	curID := getCurID(filenames)
	fileManager, err := buildFileManager(filenames)
	if err != nil {
		flock.Unlock()
		return nil, err
	}
	codecManager, err := buildCodecManager(fileManager, options.CodecFactory)
	if err != nil {
		flock.Unlock()
		return nil, err
	}

	index, err := buildIndex(fileManager, options.CodecFactory)
	if err != nil {
		flock.Unlock()
		return nil, err
	}
	db := &DB{
		options:      options,
		index:        index,
		flock:        flock,
		curID:        curID,
		fileManager:  fileManager,
		codecManager: codecManager,
		path:         path,
	}

	return db, nil

}

// Put Put
func (db *DB) Put(key, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	file := db.fileManager.Get(db.curID)
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	codec := db.codecManager.Get(db.curID)
	size := fileInfo.Size()
	if fileInfo.Size() >= db.options.MaxFileSize {
		db.curID++
		var err error
		file, err = newFile(db.path, db.curID)
		if err != nil {
			return err
		}
		db.fileManager.Add(db.curID, file)
		codec = db.options.CodecFactory(file)
		db.codecManager.Add(db.curID, codec)
	}
	entry := entry.NewEntry(key, value)
	if err := codec.Encode(entry); err != nil {
		return err
	}
	db.index.Put(string(key), &index.Item{ID: db.curID, Offset: size})
	return nil
}

// Get Get
func (db *DB) Get(key []byte) ([]byte, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	index, exist := db.index.Get(string(key))
	if exist {
		return nil, ErrKeyNotExist
	}
	codec := db.codecManager.Get(index.ID)
	entry, err := codec.DecodeAt(index.Offset)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

// Close Close
func (db *DB) Close() {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.fileManager.Close()
	db.flock.Unlock()
}
