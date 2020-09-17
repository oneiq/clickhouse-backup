package chbackup

import (
	"fmt"
	"io"
	"os"
	"path"
	"time"
)

type Dir struct {
	Config    *DirConfig
}

// Connect - "connect" to directory
func (s *Dir) Connect() error {
	fi, err := os.Stat(s.Config.Path)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("dir: path must be a directory")
	}
	return nil
}

func (s *Dir) Kind() string {
	return "dir"
}

func (s *Dir) GetFileReader(key string) (io.ReadCloser, error) {
	return os.Open(key)
}

func (s *Dir) PutFile(key string, r io.ReadCloser) error {
	f, err := os.Create(key)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func (s *Dir) DeleteFile(key string) error {
	return os.Remove(key)
}

func (s *Dir) GetFile(key string) (RemoteFile, error) {
	switch f, err := os.Stat(key); {
	case os.IsNotExist(err):
		return nil, ErrNotFound
	case err != nil:
		return nil, err
	default:
		return &dirFile{FileInfo: f, name: key}, nil
	}
}

func (s *Dir) Walk(_ string, process func(r RemoteFile)) error {
	d, err := os.Open(s.Config.Path)
	if err != nil {
		return err
	}
	defer d.Close()

	fs, err := d.Readdir(0)
	if err != nil {
		return err
	}

	for _, f := range fs {
		process(&dirFile{FileInfo: f, name: path.Join(s.Config.Path, f.Name())})
	}
	return nil
}

type dirFile struct {
	os.FileInfo
	name string
}

func (f *dirFile) Name() string {
	return f.name
}

func (f *dirFile) LastModified() time.Time {
	return f.ModTime()
}
