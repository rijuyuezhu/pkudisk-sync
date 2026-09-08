//go:build windows

package executor

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func physicalObjectIdentity(path string, _ os.FileInfo) (string, error) {
	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return "", err
	}
	h, err := windows.CreateFile(
		pathp,
		0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(h)
	var data windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &data); err != nil {
		return "", err
	}
	index := uint64(data.FileIndexHigh)<<32 | uint64(data.FileIndexLow)
	return fmt.Sprintf("windows:%d:%d", data.VolumeSerialNumber, index), nil
}
