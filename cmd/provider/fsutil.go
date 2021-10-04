package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
)

// checkWritable checks the the directory is writable.
// If the directory does not exist it is created with writable permission.
func checkWritable(dir string) error {
	if dir == "" {
		return errors.New("cannot check empty directory")
	}

	var err error
	dir, err = homedir.Expand(dir)
	if err != nil {
		return err
	}

	if _, err = os.Stat(dir); err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			// dir doesn't exist, check that we can create it
			return os.Mkdir(dir, 0o775)
		case errors.Is(err, os.ErrPermission):
			return fmt.Errorf("cannot write to %s, incorrect permissions", err)
		default:
			return err
		}
	}

	// dir exists, make sure we can write to it
	testfile := filepath.Join(dir, "test")
	fi, err := os.Create(testfile)
	if err != nil {
		if os.IsPermission(err) {
			return fmt.Errorf("%s is not writeable by the current user", dir)
		}
		return fmt.Errorf("unexpected error while checking writeablility of repo root: %s", err)
	}
	fi.Close()
	return os.Remove(testfile)
}

// fileExists checks whether the file exists.
func fileExists(filename string) bool {
	fi, err := os.Lstat(filename)
	return fi != nil || (err != nil && !errors.Is(err, os.ErrNotExist))
}
