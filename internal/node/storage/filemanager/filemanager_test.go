package filemanager

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// --- Helper Functions ---

func setupTestDir(t testing.TB) string {
	dir, err := os.MkdirTemp("", "fm_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

func cleanupTestDir(t testing.TB, dir string) {
	_ = os.RemoveAll(dir)
}

func generateRandomContent(size int) string {
	b := make([]byte, size)
	_, _ = rand.Read(b)
	return string(b)
}

// --- Functional Tests ---

func TestWriteAndReadIntegrity(t *testing.T) {
	baseDir := setupTestDir(t)
	defer cleanupTestDir(t, baseDir)

	// Initialize with 50 shards, direct_sync = false
	fm, err := NewFileManager(baseDir, 100, 50, false)
	if err != nil {
		t.Fatalf("Failed to init FM: %v", err)
	}

	fileName := "integrity.txt"
	content := "Hello, Distributed Systems!"

	if _, err := fm.WriteToFile(fileName, []byte(content)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	readData, err := fm.ReadFromFile(fileName)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(readData) != content {
		t.Errorf("Content mismatch. Got %s, want %s", string(readData), content)
	}
}

func TestHasFile(t *testing.T) {
	baseDir := setupTestDir(t)
	defer cleanupTestDir(t, baseDir)

	fm, _ := NewFileManager(baseDir, 10, 10, false)
	fileName := "exists.txt"
	_, _ = fm.WriteToFile(fileName, []byte("data"))

	if !fm.HasFile(fileName) {
		t.Errorf("HasFile returned false for existing file")
	}
	if fm.HasFile("missing.txt") {
		t.Errorf("HasFile returned true for missing file")
	}
}

func TestGetFileHash(t *testing.T) {
	baseDir := setupTestDir(t)
	defer cleanupTestDir(t, baseDir)

	fm, _ := NewFileManager(baseDir, 10, 10, false)
	fileName := "hashed.txt"
	content := "Hashing verification content"

	if _, err := fm.WriteToFile(fileName, []byte(content)); err != nil {
		t.Fatal(err)
	}

	hasher := sha256.New()
	hasher.Write([]byte(content))
	expectedHash := hex.EncodeToString(hasher.Sum(nil))

	storedHash, err := fm.GetFileHash(fileName)
	if err != nil {
		t.Fatalf("GetFileHash failed: %v", err)
	}

	if storedHash != expectedHash {
		t.Errorf("Hash mismatch.\nExp: %s\nGot: %s", expectedHash, storedHash)
	}
}

func TestDynamicSharding(t *testing.T) {
	baseDir := setupTestDir(t)
	defer cleanupTestDir(t, baseDir)

	// Tiny shard count to force collisions
	shardCount := uint32(2)
	fm, _ := NewFileManager(baseDir, 10, shardCount, false)

	fileName := "shard_test.txt"
	if _, err := fm.WriteToFile(fileName, []byte("data")); err != nil {
		t.Fatal(err)
	}

	// Verify file physically exists in folder "0" or "1"
	found := false
	for i := 0; i < int(shardCount); i++ {
		path := filepath.Join(baseDir, fmt.Sprintf("%d", i), fileName)
		if _, err := os.Stat(path); err == nil {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("File not found in expected shards")
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	baseDir := setupTestDir(t)
	defer cleanupTestDir(t, baseDir)

	fm, _ := NewFileManager(baseDir, 4096, 100, false)

	var wg sync.WaitGroup
	workers := 20
	filesPerWorker := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < filesPerWorker; j++ {
				fileName := fmt.Sprintf("worker_%d_file_%d", id, j)
				content := fmt.Sprintf("data_%d_%d", id, j)

				if _, err := fm.WriteToFile(fileName, []byte(content)); err != nil {
					t.Errorf("Write failed: %v", err)
					return
				}
				readVal, err := fm.ReadFromFile(fileName)
				if err != nil {
					t.Errorf("Read failed: %v", err)
					return
				}
				if string(readVal) != content {
					t.Errorf("Data corruption")
				}
			}
		}(i)
	}
	wg.Wait()
}

// --- BENCHMARKS ---

// BenchmarkWriteToFile: Single core sequential write speed.
func BenchmarkWriteToFile(b *testing.B) {
	benchmarks := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10240},
		{"1MB", 1024 * 1024},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			baseDir := setupTestDir(b)
			defer cleanupTestDir(b, baseDir)

			fm, _ := NewFileManager(baseDir, 4096, 100, false)
			content := generateRandomContent(bm.size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fileName := "bench_" + strconv.Itoa(i) + ".dat"
				if _, err := fm.WriteToFile(fileName, []byte(content)); err != nil {
					b.Fatalf("Write failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkParallelWrite: Multi-core write throughput.
// Run with: go test -bench=ParallelWrite -cpu=1,4,8
func BenchmarkParallelWrite(b *testing.B) {
	baseDir := setupTestDir(b)
	defer cleanupTestDir(b, baseDir)

	fm, _ := NewFileManager(baseDir, 4096, 1000, false)
	content := generateRandomContent(1024)
	var counter int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddInt64(&counter, 1)
			fileName := "pwrite_" + strconv.FormatInt(id, 10) + ".dat"
			if _, err := fm.WriteToFile(fileName, []byte(content)); err != nil {
				b.Errorf("Write failed: %v", err)
			}
		}
	})
}

// BenchmarkReadFromFile: Single core sequential read.
func BenchmarkReadFromFile(b *testing.B) {
	baseDir := setupTestDir(b)
	defer cleanupTestDir(b, baseDir)

	fm, _ := NewFileManager(baseDir, 4096, 100, false)
	content := generateRandomContent(1024)
	fileName := "static_read.dat"
	_, _ = fm.WriteToFile(fileName, []byte(content))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fm.ReadFromFile(fileName); err != nil {
			b.Fatalf("Read failed: %v", err)
		}
	}
}

// BenchmarkParallelRead: Multi-core random read throughput.
func BenchmarkParallelRead(b *testing.B) {
	baseDir := setupTestDir(b)
	defer cleanupTestDir(b, baseDir)

	fm, _ := NewFileManager(baseDir, 4096, 100, false)
	content := generateRandomContent(1024)

	const fileCount = 1000
	for i := 0; i < fileCount; i++ {
		_, _ = fm.WriteToFile(fmt.Sprintf("file_%d.dat", i), []byte(content))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			fileName := fmt.Sprintf("file_%d.dat", i%fileCount)
			if _, err := fm.ReadFromFile(fileName); err != nil {
				b.Errorf("Read failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkGetFileHash: Measures CPU + IO cost of hashing.
func BenchmarkGetFileHash(b *testing.B) {
	baseDir := setupTestDir(b)
	defer cleanupTestDir(b, baseDir)

	fm, _ := NewFileManager(baseDir, 4096, 100, false)
	content := generateRandomContent(10240) // 10KB
	fileName := "hash_bench.dat"
	_, _ = fm.WriteToFile(fileName, []byte(content))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fm.GetFileHash(fileName); err != nil {
			b.Fatalf("Hash failed: %v", err)
		}
	}
}
