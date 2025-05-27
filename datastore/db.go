package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

var simulateMergeError = false

type hashIndex map[string]int64

type segmentInfo struct {
	file   string
	offset int64
}

type Db struct {
	dir         string
	out         *os.File
	outOffset   int64
	segmentSize int64
	segmentNum  int
	
	index    hashIndex
	segments map[string]*segmentInfo
	mu       sync.RWMutex
}

func Open(dir string, segmentSize int64) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	
	db := &Db{
		dir:         dir,
		out:         f,
		segmentSize: segmentSize,
		index:       make(hashIndex),
		segments:    make(map[string]*segmentInfo),
	}
	
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	
	return db, nil
}

func (db *Db) recover() error {
	f, err := os.Open(db.out.Name())
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	for err == nil {
		var (
			record entry
			n      int
		)
		n, err = record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted file")
			}
			break
		}

		db.index[record.key] = db.outOffset
		db.outOffset += int64(n)
	}
	
	pattern := filepath.Join(db.dir, "*.segment")
	segmentFiles, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	
	sort.Strings(segmentFiles)
	
	for _, segmentFile := range segmentFiles {
		err = db.recoverFromSegment(segmentFile)
		if err != nil {
			return err
		}
		
		base := filepath.Base(segmentFile)
		if strings.HasSuffix(base, ".segment") {
			numStr := strings.TrimSuffix(base, ".segment")
			if num, parseErr := strconv.Atoi(numStr); parseErr == nil && num >= db.segmentNum {
				db.segmentNum = num + 1
			}
		}
	}
	
	return nil
}

func (db *Db) recoverFromSegment(segmentFile string) error {
	f, err := os.Open(segmentFile)
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	var offset int64
	
	for {
		var (
			record entry
			n      int
		)
		n, err = record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted segment file: %s", segmentFile)
			}
			break
		}
		if err != nil {
			return err
		}

		db.segments[record.key] = &segmentInfo{
			file:   segmentFile,
			offset: offset,
		}
		
		delete(db.index, record.key)
		
		offset += int64(n)
	}
	
	return nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	if segInfo, ok := db.segments[key]; ok {
		return db.getFromSegment(segInfo.file, segInfo.offset)
	}
	
	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	file, err := os.Open(db.out.Name())
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) getFromSegment(segmentFile string, offset int64) (string, error) {
	file, err := os.Open(segmentFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		return "", err
	}

	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	e := entry{
		key:   key,
		value: value,
	}
	
	encoded := e.Encode()
	
	if db.segmentSize > 0 && db.outOffset+int64(len(encoded)) > db.segmentSize {
		if err := db.createNewSegment(); err != nil {
			return err
		}
	}
	
	n, err := db.out.Write(encoded)
	if err == nil {
		delete(db.segments, key)
		
		db.index[key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

func (db *Db) createNewSegment() error {
	if err := db.out.Close(); err != nil {
		return err
	}
	
	currentPath := db.out.Name()
	segmentPath := filepath.Join(db.dir, fmt.Sprintf("%d.segment", db.segmentNum))
	
	if err := os.Rename(currentPath, segmentPath); err != nil {
		return err
	}
	
	for key, offset := range db.index {
		db.segments[key] = &segmentInfo{
			file:   segmentPath,
			offset: offset,
		}
	}
	
	db.index = make(hashIndex)
	db.segmentNum++
	
	f, err := os.OpenFile(currentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	
	db.out = f
	db.outOffset = 0
	
	go db.MergeSegments()
	
	return nil
}

func (db *Db) MergeSegments() {
	if simulateMergeError {
		return
	}
	
	db.mu.Lock()
	defer db.mu.Unlock()
	
	pattern := filepath.Join(db.dir, "*.segment")
	segmentFiles, err := filepath.Glob(pattern)
	if err != nil || len(segmentFiles) < 2 {
		return
	}
	
	tempFile := filepath.Join(db.dir, "merge.tmp")
	f, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return
	}
	
	allKeys := make(map[string]string)
	
	sort.Sort(sort.Reverse(sort.StringSlice(segmentFiles)))
	
	for _, segmentFile := range segmentFiles {
		segFile, err := os.Open(segmentFile)
		if err != nil {
			f.Close()
			os.Remove(tempFile)
			return
		}
		
		in := bufio.NewReader(segFile)
		for {
			var record entry
			_, err := record.DecodeFromReader(in)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				segFile.Close()
				f.Close()
				os.Remove(tempFile)
				return
			}
			
			if _, exists := allKeys[record.key]; !exists {
				allKeys[record.key] = record.value
			}
		}
		segFile.Close()
	}
	
	newSegments := make(map[string]*segmentInfo)
	var offset int64
	
	for key, value := range allKeys {
		e := entry{key: key, value: value}
		encoded := e.Encode()
		
		if _, err := f.Write(encoded); err != nil {
			f.Close()
			os.Remove(tempFile)
			return
		}
		
		newSegments[key] = &segmentInfo{
			file:   tempFile,
			offset: offset,
		}
		offset += int64(len(encoded))
	}
	
	f.Close()
	
	mergedSegmentPath := filepath.Join(db.dir, fmt.Sprintf("%d.segment", db.segmentNum))
	if err := os.Rename(tempFile, mergedSegmentPath); err != nil {
		os.Remove(tempFile)
		return
	}
	
	for key := range newSegments {
		if segInfo, exists := db.segments[key]; exists {
			segInfo.file = mergedSegmentPath
			segInfo.offset = newSegments[key].offset
		}
	}
	
	for _, segmentFile := range segmentFiles {
		os.Remove(segmentFile)
	}
	
	db.segmentNum++
}

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	
	size := info.Size()
	
	pattern := filepath.Join(db.dir, "*.segment")
	segmentFiles, err := filepath.Glob(pattern)
	if err != nil {
		return size, nil
	}
	
	for _, segmentFile := range segmentFiles {
		if segInfo, err := os.Stat(segmentFile); err == nil {
			size += segInfo.Size()
		}
	}
	
	return size, nil
}