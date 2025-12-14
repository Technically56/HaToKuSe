package filemanager

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// --- Helper Functions ---

// setupTestDir creates a temporary directory for testing and returns the path.
func setupTestDir(t testing.TB) string {
	dir, err := os.MkdirTemp("", "filemanager_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// cleanupTestDir removes the temporary directory and all its contents.
func cleanupTestDir(t testing.TB, dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Errorf("Failed to cleanup temp dir: %v", err)
	}
}

// generateRandomContent creates a random string of a specific size (in bytes).
func generateRandomContent(size int) string {
	b := make([]byte, size)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// --- Tests ---

// TestConcurrentWrites simulates high concurrency to check for race conditions
// and ensures the atomic file counter is accurate.
func TestConcurrentWrites(t *testing.T) {
	baseDir := setupTestDir(t)
	defer cleanupTestDir(t, baseDir)

	// Initialize FileManager with a small lock pool to force contention
	lockPoolSize := int32(16)
	fm, err := NewFileManager(baseDir, lockPoolSize)
	if err != nil {
		t.Fatalf("Failed to create FileManager: %v", err)
	}

	const numGoroutines = 50
	const filesPerRoutine = 20
	totalFiles := numGoroutines * filesPerRoutine

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Channel to catch errors from goroutines
	errChan := make(chan error, totalFiles)

	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < filesPerRoutine; j++ {
				fileName := fmt.Sprintf("file_%d_%d.txt", routineID, j)
				content := fmt.Sprintf("Routine %d Data %d", routineID, j)

				if err := fm.WriteToFile(fileName, content); err != nil {
					errChan <- fmt.Errorf("Routine %d write error: %v", routineID, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// 1. Check for write errors
	for err := range errChan {
		t.Error(err)
	}

	// 2. Verify Atomic Counter
	// Note: Since we started with an empty folder, the counter should match total writes.
	if currentCount := fm.GetFileCounter(); currentCount != int64(totalFiles) {
		t.Errorf("Counter mismatch. Expected %d, got %d", totalFiles, currentCount)
	}

	// 3. Verify actual files on disk
	var diskFileCount int64 = 0
	err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			diskFileCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk directory: %v", err)
	}

	if diskFileCount != int64(totalFiles) {
		t.Errorf("Disk file count mismatch. Expected %d, found %d", totalFiles, diskFileCount)
	}
}

// TestWriteIntegrity checks that data is written to the correct folder
// and the content is exactly what we expect.
func TestWriteIntegrity(t *testing.T) {
	baseDir := setupTestDir(t)
	defer cleanupTestDir(t, baseDir)

	fm, _ := NewFileManager(baseDir, 10)

	fileName := "integrity_check.txt"
	content := "This is a specific string to check integrity."

	// This should be the first file, so it goes to folder "0"
	if err := fm.WriteToFile(fileName, content); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}

	// Calculate expected path based on logic: (count 1 - 1) / 1000 = 0
	expectedSubDir := "0"
	fullPath := filepath.Join(baseDir, expectedSubDir, fileName)

	// Read file back
	readBytes, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("Failed to read file at %s: %v", fullPath, err)
	}

	if string(readBytes) != content {
		t.Errorf("Content mismatch.\nExpected: %s\nGot: %s", content, string(readBytes))
	}
}

// --- Benchmarks ---

// BenchmarkWriteToFile measures performance for different file sizes.
// It resets the environment for each sub-benchmark.
func BenchmarkWriteToFile(b *testing.B) {
	benchmarks := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Setup per benchmark iteration
			baseDir := setupTestDir(b)
			defer cleanupTestDir(b, baseDir)

			fm, _ := NewFileManager(baseDir, 100)
			content := generateRandomContent(bm.size)

			b.ResetTimer() // Start timing now

			for i := 0; i < b.N; i++ {
				// We use unique names to simulate real logging/appending behavior
				// rather than constantly overwriting the same file which might be cached by OS.
				fileName := fmt.Sprintf("bench_%d.log", i)
				if err := fm.WriteToFile(fileName, content); err != nil {
					b.Fatalf("Write failed: %v", err)
				}
			}
		})
	}
}
