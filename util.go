package bitcask

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/wwq1988/bitcask/internal/codec"
	"github.com/wwq1988/bitcask/internal/codecmanager"
	"github.com/wwq1988/bitcask/internal/filemanager"
	"github.com/wwq1988/bitcask/internal/index"
)

func ensureDir(path string) error {
	if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	return nil
}

// getFilenames getFilenames
func getFilenames(path string) ([]string, error) {
	filenames, err := filepath.Glob(fmt.Sprintf("%s/*.db", path))
	if err != nil {
		return nil, err
	}
	return filenames, nil
}

func getCurID(filenames []string) int64 {
	var curID int64 = -1
	for _, filename := range filenames {
		base := filepath.Base(filename)
		id, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			continue
		}
		if id > curID {
			curID = id
		}
	}
	return curID
}

func buildFileManager(filenames []string) (filemanager.FileManager, error) {
	fileManager := filemanager.New()
	files2Close := make([]*os.File, 0, len(filenames))
	for _, filename := range filenames {
		base := filepath.Base(filename)
		id, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			continue
		}
		file, err := os.Open(filename)
		if err != nil {
			for _, file2Close := range files2Close {
				file2Close.Close()
			}
			return nil, err
		}
		files2Close = append(files2Close, file)
		fileManager.Add(id, file)
	}
	return fileManager, nil
}

func buildIndex(fileManager filemanager.FileManager, codecFactory codec.Factory) (index.Index, error) {
	idx := index.New()
	err := fileManager.Iter(func(id int64, file *os.File) error {
		codec := codecFactory(file)
		for {
			entry, err := codec.Decode()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			idx.Put(string(entry.Key), &index.Item{ID: id, Offset: entry.Offset})
		}
	})
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func buildCodecManager(fileManager filemanager.FileManager, codecFactory codec.Factory) (codecmanager.CodecManager, error) {
	cm := codecmanager.New()
	err := fileManager.Iter(func(id int64, file *os.File) error {
		codec := codecFactory(file)
		cm.Add(id, codec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cm, nil
}

func newFile(path string, id int64) (*os.File, error) {
	fullPath := filepath.Join(path, fmt.Sprintf("%d.db", id))
	file, err := os.Create(fullPath)
	if err != nil {
		return nil, err
	}
	return file, nil
}
