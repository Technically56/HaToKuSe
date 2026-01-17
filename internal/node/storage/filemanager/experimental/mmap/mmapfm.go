package mmap

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

type FileManagerInterface interface {
	WriteToFile(file_name string, file_content []byte) (string, error)
	ReadFromFile(file_name string) ([]byte, error)
	HasFile(file_name string) bool
	GetFileHash(file_name string) (string, error)
	GetFileCounter() int64
	GetFolderCounter() int64
}

type FileManager struct {
	folder_path  string
	file_locks   []*sync.RWMutex
	file_count   int64
	folder_count int64
	createdDirs  sync.Map
	shard_count  uint32
	direct_sync  bool
}

func NewFileManager(folder_path string, lock_pool_size int32, shard_count uint32, direct_sync bool) (*FileManager, error) {
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
	return &FileManager{
		folder_path:  folder_path,
		folder_count: int64(folder_count),
		file_count:   int64(file_count),
		file_locks:   locks,
		shard_count:  shard_count,
		direct_sync:  direct_sync,
	}, nil
}

func (fm *FileManager) getFileLock(file_path string) *sync.RWMutex {
	h := fnv.New32a()
	h.Write([]byte(file_path))
	return fm.file_locks[h.Sum32()%uint32(len(fm.file_locks))]
}

func (fm *FileManager) WriteToFile(file_name string, file_content []byte) (string, error) {
	hash := fnv.New32a()
	hash.Write([]byte(file_name))
	folder_id := hash.Sum32() % fm.shard_count

	sub_directory := fmt.Sprintf("%d", folder_id)
	dir_path := filepath.Join(fm.folder_path, sub_directory)
	full_path := filepath.Join(dir_path, file_name)

	if _, ok := fm.createdDirs.Load(sub_directory); !ok {
		os.MkdirAll(dir_path, 0755)
		fm.createdDirs.Store(sub_directory, true)
	}

	lock := fm.getFileLock(full_path)
	lock.Lock()
	defer lock.Unlock()

	f, err := os.OpenFile(full_path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}

	fileSize := int64(len(file_content))

	if fileSize > 0 {
		if err := f.Truncate(fileSize); err != nil {
			f.Close()
			return "", err
		}

		mmapData, err := unix.Mmap(int(f.Fd()), 0, int(fileSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			f.Close()
			return "", fmt.Errorf("mmap error: %v", err)
		}

		copy(mmapData, file_content)

		if fm.direct_sync {
			if err := unix.Msync(mmapData, unix.MS_SYNC); err != nil {
				fmt.Println("Msync error:", err)
			}
		}

		if err := unix.Munmap(mmapData); err != nil {
			f.Close()
			return "", fmt.Errorf("munmap error: %v", err)
		}
	}

	if err := f.Close(); err != nil {
		return "", err
	}

	hasher := sha256.New()
	hasher.Write(file_content)
	fm.increaseFileCounter()

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (fm *FileManager) ReadFromFile(file_name string) ([]byte, error) {
	h := fnv.New32a()
	h.Write([]byte(file_name))
	folderID := h.Sum32() % fm.shard_count
	sub_directory := fmt.Sprintf("%d", folderID)

	full_path := filepath.Join(fm.folder_path, sub_directory, file_name)

	lock := fm.getFileLock(full_path)
	lock.RLock()
	defer lock.RUnlock()

	data, err := os.ReadFile(full_path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (fm *FileManager) increaseFileCounter() {
	atomic.AddInt64(&fm.file_count, 1)
}
func (fm *FileManager) GetFileCounter() int64 {
	return atomic.LoadInt64(&fm.file_count)
}
func (fm *FileManager) GetFolderCounter() int64 {
	return atomic.LoadInt64(&fm.folder_count)
}
func (fm *FileManager) HasFile(file_name string) bool {
	h := fnv.New32a()
	h.Write([]byte(file_name))
	folderID := h.Sum32() % fm.shard_count
	sub_directory := fmt.Sprintf("%d", folderID)

	full_path := filepath.Join(fm.folder_path, sub_directory, file_name)
	if _, err := os.Stat(full_path); err == nil {
		return true
	}
	return false
}
func (fm *FileManager) GetFileHash(file_name string) (string, error) {
	h := fnv.New32a()
	h.Write([]byte(file_name))
	folderID := h.Sum32() % fm.shard_count
	sub_directory := fmt.Sprintf("%d", folderID)

	full_path := filepath.Join(fm.folder_path, sub_directory, file_name)
	lock := fm.getFileLock(full_path)
	lock.RLock()
	defer lock.RUnlock()

	file, err := os.Open(full_path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	hashBytes := hasher.Sum(nil)
	return hex.EncodeToString(hashBytes), nil
}
