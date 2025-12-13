package filemanager

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

// Writer struct to be defined to the default messages directory of the running server.
type FileManager struct {
	folder_path  string
	file_locks   sync.Map
	file_count   uint64
	folder_count uint64
}

func (fm *FileManager) getFileLock(file_path string) *sync.RWMutex {
	newLock := &sync.RWMutex{}

	realLock, _ := fm.file_locks.LoadOrStore(file_path, newLock)

	return realLock.(*sync.RWMutex)
}

func newFileManager(folder_path string) *FileManager {
	return &FileManager{folder_path: folder_path}
}

func (fm *FileManager) writeToFile(file_name string, file_content string) error {
	full_path := fm.folder_path + file_name
	tmp_path := full_path + ".tmp"

	lock := fm.getFileLock(full_path)

	lock.Lock()

	defer lock.Unlock()

	file, err := os.OpenFile(tmp_path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	if _, err = writer.WriteString(file_content); err != nil {
		//Add Error Logging To The Leader Here
		fmt.Println("Error writing to buffer:", err)
		return err
	}

	if err := writer.Flush(); err != nil {
		//Add Error Logging To The Leader Here
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}
	return os.Rename(tmp_path, full_path)
}
