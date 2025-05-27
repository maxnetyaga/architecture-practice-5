package datastore

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp, 0)
		if err != nil {
			t.Fatal(err)
		}

		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}

		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})
}

func TestDbSegmentation(t *testing.T) {
	tmp := t.TempDir()

	db, err := Open(tmp, 100)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	largeData := make([]byte, 50)
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		err := db.Put(key, string(largeData))
		if err != nil {
			t.Fatalf("Cannot put %s: %s", key, err)
		}
	}

	t.Run("segment creation", func(t *testing.T) {
		files, err := filepath.Glob(filepath.Join(tmp, "*.segment"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) < 2 {
			t.Errorf("Expected multiple segments, got %d", len(files))
		}
	})

	t.Run("read from multiple segments", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := string(rune('a' + i))
			_, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
		}
	})

	t.Run("merge operation", func(t *testing.T) {
		// wait for merge to complete
		time.Sleep(2 * time.Second)

		files, err := filepath.Glob(filepath.Join(tmp, "*.segment"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) > 1 {
			t.Errorf("Merge may not have happened yet, or implementation keeps multiple segments")
		}

		for i := 0; i < 10; i++ {
			key := string(rune('a' + i))
			_, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s after merge: %s", key, err)
			}
		}
	})
}

func TestDbMergeAtomicity(t *testing.T) {
	tmp := t.TempDir()

	db, err := Open(tmp, 100)
	if err != nil {
		t.Fatal(err)
	}

	simulateMergeError = true
	defer func() { simulateMergeError = false }()

	largeValue := strings.Repeat("x", 50) // Half of segment size
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		if err := db.Put(key, largeValue); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	initialSegments := countSegments(t, tmp)

	currentSegments := countSegments(t, tmp)
	if currentSegments != initialSegments {
		t.Errorf("Segment count changed from %d to %d after failed merge",
			initialSegments, currentSegments)
	}

	if hasMergeTempFiles(t, tmp) {
		t.Error("Temporary merge files remain after failure")
	}

	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		if val, err := db.Get(key); err != nil || val != largeValue {
			t.Errorf("Data corruption on key %s (err: %v, val: %q)", key, err, val)
		}
	}
}

func countSegments(t *testing.T, dir string) int {
	files, err := filepath.Glob(filepath.Join(dir, "*.segment"))
	if err != nil {
		t.Fatal(err)
	}
	return len(files)
}

func hasMergeTempFiles(t *testing.T, dir string) bool {
	files, err := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	return len(files) > 0
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp, 20)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	initialPairs := [][]string{
		{"key1", "initial_value1"},
		{"key2", "initial_value2"},
		{"key3", "initial_value3"},
	}

	for _, pair := range initialPairs {
		if err := db.Put(pair[0], pair[1]); err != nil {
			t.Fatalf("Failed to put initial data: %v", err)
		}
	}

	const numReaders = 10
	const numWriters = 5
	const numOperationsPerGoroutine = 100

	var wg sync.WaitGroup
	errors := make(chan error, numReaders+numWriters)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine; j++ {
				key := fmt.Sprintf("key%d", (j%3)+1)
				value, err := db.Get(key)
				if err != nil {
					errors <- fmt.Errorf("reader %d: failed to get %s: %v", readerID, key, err)
					return
				}

				if value != fmt.Sprintf("initial_value%d", (j%3)+1) &&
					value != fmt.Sprintf("updated_value%d", (j%3)+1) {
					errors <- fmt.Errorf("reader %d: unexpected value for %s: %s", readerID, key, value)
					return
				}
			}
		}(i)
	}

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine; j++ {
				key := fmt.Sprintf("key%d", (j%3)+1)
				value := fmt.Sprintf("updated_value%d", (j%3)+1)
				if err := db.Put(key, value); err != nil {
					errors <- fmt.Errorf("writer %d: failed to put %s: %v", writerID, key, err)
					return
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errors:
		t.Fatalf("Concurrent operation failed: %v", err)
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out")
	}

	for i := 1; i <= 3; i++ {
		key := fmt.Sprintf("key%d", i)
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to get final value for %s: %v", key, err)
		}
		expectedInitial := fmt.Sprintf("initial_value%d", i)
		expectedUpdated := fmt.Sprintf("updated_value%d", i)
		if value != expectedInitial && value != expectedUpdated {
			t.Errorf("Unexpected final value for %s: %s", key, value)
		}
	}
}
