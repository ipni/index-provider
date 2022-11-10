package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/mitchellh/go-homedir"
)

// checkWritable checks the the directory is writable. If the directory does
// not exist it is created with writable permission.
func checkWritable(dir string) error {
	if dir == "" {
		return errors.New("directory not specified")
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
			return fmt.Errorf("cannot write to %s, incorrect permissions", dir)
		default:
			return err
		}
	}

	// dir exists, make sure we can write to it
	file, err := os.CreateTemp(dir, "test")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s is not writeable by the current user", dir)
		}
		return fmt.Errorf("error checking writeablility of directory %s: %w", dir, err)
	}
	file.Close()
	return os.Remove(file.Name())
}

// fileExists checks whether the file exists.
func fileExists(filename string) bool {
	_, err := os.Lstat(filename)
	return !errors.Is(err, os.ErrNotExist)
}
