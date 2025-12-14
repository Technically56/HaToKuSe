package filemanager

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Writer struct to be defined to the default messages directory of the running server.
type FileManager struct {
	folder_path  string
	file_locks   []*sync.RWMutex
	file_count   int64
	folder_count int64
}

func NewFileManager(folder_path string, lock_pool_size int32) (*FileManager, error) {
	if _, err := os.Stat(folder_path); os.IsNotExist(err) {
		return nil, fmt.Errorf("base path does not exist: %s", folder_path)
	}
	dirs, err := os.ReadDir(folder_path)
	if err != nil {
		return nil, err
	}
	folder_count := len(dirs)
	file_count := 0
	for _, dir_element := range dirs {
		if dir_element.IsDir() {
			recDirs, _ := os.ReadDir(filepath.Join(folder_path, dir_element.Name()))
			file_count += len(recDirs)
		}

	}
	locks := make([]*sync.RWMutex, lock_pool_size)
	for i := range locks {
		locks[i] = &sync.RWMutex{}
	}
	return &FileManager{folder_path: folder_path, folder_count: int64(folder_count), file_count: int64(file_count), file_locks: locks}, nil
}

// file_path must be the full path to the file
func (fm *FileManager) getFileLock(file_path string) *sync.RWMutex {
	h := fnv.New32a()
	h.Write([]byte(file_path))
	return fm.file_locks[h.Sum32()%uint32(len(fm.file_locks))]
}

func (fm *FileManager) WriteToFile(file_name string, file_content string) error {
	current_counter := atomic.AddInt64(&fm.file_count, 1) - 1

	sub_directory := fmt.Sprintf("%d", current_counter/1000)

	dir_path := filepath.Join(fm.folder_path, sub_directory)

	full_path := filepath.Join(dir_path, file_name)

	tmp_path := full_path + ".tmp"

	if err := os.MkdirAll(dir_path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	lock := fm.getFileLock(full_path)

	lock.Lock()

	defer lock.Unlock()

	file, err := os.OpenFile(tmp_path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}

	writer := bufio.NewWriter(file)

	if _, err = writer.WriteString(file_content); err != nil {
		//Add Error Logging To The Leader Here
		fmt.Println("Error writing to buffer:", err)
		file.Close()
		return err
	}

	if err := writer.Flush(); err != nil {
		//Add Error Logging To The Leader Here
		file.Close()
		return err
	}

	//if err := file.Sync(); err != nil {
	//Add Error Logging To The Leader Here
	//return err
	//}

	if err := file.Close(); err != nil {
		//Add Error Logging To The Leader Here
		return err
	}

	return os.Rename(tmp_path, full_path)
}

func (fm *FileManager) increaseFileCounter() {
	atomic.AddInt64(&fm.file_count, 1)
}
func (fm *FileManager) GetFileCounter() int64 {
	return fm.file_count
}
func (fm *FileManager) GetFolderCounter() int64 {
	return fm.folder_count
}
