package chbackup

import (
	"archive/tar"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mholt/archiver"
	"gopkg.in/djherbis/buffer.v1"
	"gopkg.in/djherbis/nio.v2"
)

const (
	// MetaFileName - meta file name
	MetaFileName = "meta.json"
	MetaRequiredBackupField = "required_backup"
	MetaHardlinksField      = "hardlinks"
	// BufferSize - size of ring buffer between stream handlers
	BufferSize = 4 * 1024 * 1024
)

// MetaFile - structure describe meta file that will be added to incremental backups archive.
// Contains info of required files in backup and files
type MetaFile struct {
	RequiredBackup string   `json:"required_backup"`
	Hardlinks      []string `json:"hardlinks"`
}

var (
	// ErrNotFound is returned when file/object cannot be found
	ErrNotFound = errors.New("file not found")
)

// RemoteFile - interface describe file on remote storage
type RemoteFile interface {
	Size() int64
	Name() string
	LastModified() time.Time
}

// RemoteStorage -
type RemoteStorage interface {
	Kind() string
	GetFile(string) (RemoteFile, error)
	DeleteFile(string) error
	Connect() error
	Walk(string, func(RemoteFile)) error
	GetFileReader(key string) (io.ReadCloser, error)
	PutFile(key string, r io.ReadCloser) error
}

type BackupDestination struct {
	RemoteStorage
	path               string
	compressionFormat  string
	compressionLevel   int
	disableProgressBar bool
	backupsToKeep      int
}

func (bd *BackupDestination) RemoveOldBackups(keep int) error {
	if keep < 1 {
		return nil
	}
	backupList, err := bd.BackupList()
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDelete(backupList, keep)
	for _, backupToDelete := range backupsToDelete {
		if err := bd.RemoveBackup(backupToDelete.Name); err != nil {
			return err
		}
	}
	return nil
}

func (bd *BackupDestination) RemoveBackup(backupName string) error {
	objects := []string{}
	if err := bd.Walk(bd.path, func(f RemoteFile) {
		if strings.HasPrefix(f.Name(), path.Join(bd.path, backupName)) {
			objects = append(objects, f.Name())
		}
	}); err != nil {
		return err
	}
	for _, key := range objects {
		err := bd.DeleteFile(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bd *BackupDestination) BackupsToKeep() int {
	return bd.backupsToKeep
}

func (bd *BackupDestination) BackupList() ([]Backup, error) {
	type ClickhouseBackup struct {
		Metadata bool
		Shadow   bool
		Tar      bool
		Size     int64
		Date     time.Time
	}
	files := map[string]ClickhouseBackup{}
	path := bd.path
	err := bd.Walk(path, func(o RemoteFile) {
		if strings.HasPrefix(o.Name(), path) {
			key := strings.TrimPrefix(o.Name(), path)
			key = strings.TrimPrefix(key, "/")
			parts := strings.Split(key, "/")

			if isArchiveExtension(parts[0]) {
				files[parts[0]] = ClickhouseBackup{
					Tar:  true,
					Date: o.LastModified(),
					Size: o.Size(),
				}
			}

			if len(parts) > 1 {
				b := files[parts[0]]
				files[parts[0]] = ClickhouseBackup{
					Metadata: b.Metadata || parts[1] == "metadata",
					Shadow:   b.Shadow || parts[1] == "shadow",
					Date:     b.Date,
					Size:     b.Size,
				}
			}
		}
	})
	if err != nil {
		return nil, err
	}
	result := []Backup{}
	for name, e := range files {
		if e.Metadata && e.Shadow || e.Tar {
			result = append(result, Backup{
				Name: name,
				Date: e.Date,
				Size: e.Size,
			})
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Date.Before(result[j].Date)
	})
	return result, nil
}

func (bd *BackupDestination) CompressedStreamDownload(remotePath string, localPath string, downloadRequired func(string) (string, error)) error {
	if err := os.MkdirAll(localPath, os.ModePerm); err != nil {
		return err
	}

	var archiveName string
	compressionFormat, _ := getArchiveFormat(remotePath)
	if compressionFormat == "" {
		compressionFormat = bd.compressionFormat
		archiveName = path.Join(bd.path, fmt.Sprintf("%s.%s", remotePath, getExtension(bd.compressionFormat)))
	} else {
		archiveName = path.Join(bd.path, remotePath)
	}

	if err := bd.Connect(); err != nil {
		return err
	}

	// get this first as GetFileReader blocks the ftp control channel
	file, err := bd.GetFile(archiveName)
	if err != nil {
		return err
	}
	filesize := file.Size()

	reader, err := bd.GetFileReader(archiveName)
	if err != nil {
		return err
	}
	defer reader.Close()

	bar := StartNewByteBar(!bd.disableProgressBar, filesize)
	buf := buffer.New(BufferSize)
	bufReader := nio.NewReader(reader, buf)
	proxyReader := bar.NewProxyReader(bufReader)
	z, _ := getArchiveReader(compressionFormat)
	if err := z.Open(proxyReader, 0); err != nil {
		return err
	}
	defer z.Close()
	var metafile MetaFile
	for {
		file, err := z.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		header, ok := file.Header.(*tar.Header)
		if !ok {
			return fmt.Errorf("expected header to be *tar.Header but was %T", file.Header)
		}
		if header.Name == MetaFileName {
			b, err := ioutil.ReadAll(file)
			if err != nil {
				return fmt.Errorf("can't read %s", MetaFileName)
			}
			if err := json.Unmarshal(b, &metafile); err != nil {
				return err
			}
			continue
		}
		extractFile := filepath.Join(localPath, header.Name)
		extractDir := filepath.Dir(extractFile)
		if _, err := os.Stat(extractDir); os.IsNotExist(err) {
			os.MkdirAll(extractDir, os.ModePerm)
		}
		dst, err := os.Create(extractFile)
		if err != nil {
			return err
		}
		if _, err := io.Copy(dst, file); err != nil {
			return err
		}
		if err := dst.Close(); err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	bar.Finish()
	var requiredLocalPath string
	if metafile.RequiredBackup != "" {
		log.Printf("Backup '%s' required '%s'.", remotePath, metafile.RequiredBackup)
		requiredLocalPath, err = downloadRequired(metafile.RequiredBackup)
		var message string
		if err == nil {
			message = "Downloaded %s. Relinking files..."
		} else if os.IsExist(err) {
			message = "%s already exists locally. Relinking files..."
		} else {
			return fmt.Errorf("can't download '%s': %v", metafile.RequiredBackup, err)
		}
		log.Printf(message, metafile.RequiredBackup)
	}
	for _, hardlink := range metafile.Hardlinks {
		newname := filepath.Join(localPath, hardlink)
		extractDir := filepath.Dir(newname)
		oldname := filepath.Join(requiredLocalPath, hardlink)
		if _, err := os.Stat(extractDir); os.IsNotExist(err) {
			os.MkdirAll(extractDir, os.ModePerm)
		}
		if err := os.Link(oldname, newname); err != nil {
			return err
		}
	}
	return nil
}

func (bd *BackupDestination) CompressedStreamUpload(localPath, remotePath, diffFromPath string) error {
	archiveName := path.Join(bd.path, fmt.Sprintf("%s.%s", remotePath, getExtension(bd.compressionFormat)))

	if _, err := bd.GetFile(archiveName); err != nil {
		if err != ErrNotFound {
			return err
		}
	}

	bar := StartNewByteBar(!bd.disableProgressBar, 0)
	if !bd.disableProgressBar {
		go func() {
			var totalBytes int64
			filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
				if info.Mode().IsRegular() {
					totalBytes += info.Size()
				}
				return nil
			})
			bar.SetTotal64(totalBytes)
		}()
	}
	if diffFromPath != "" {
		fi, err := os.Stat(diffFromPath)
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("'%s' is not a directory", diffFromPath)
		}
		if isClickhouseShadow(filepath.Join(diffFromPath, "shadow")) {
			return fmt.Errorf("'%s' is old format backup and doesn't supports diff", filepath.Base(diffFromPath))
		}
	}

	body, w := nio.Pipe(buffer.New(BufferSize))
	go func() (ferr error) {
		defer w.CloseWithError(ferr)

		mf, err := ioutil.TempFile("", MetaFileName)
		if err != nil {
			return fmt.Errorf("can't create meta.info: %v", err)
		}
		defer os.Remove(mf.Name())
		defer mf.Close()
		metawr := bufio.NewWriterSize(mf, BufferSize)
		if diffFromPath != "" {
			metawr.WriteString("{\"")
			metawr.WriteString(MetaRequiredBackupField)
			metawr.WriteString("\":\"")
			writeJsonString(metawr, filepath.Base(diffFromPath))
			metawr.WriteString("\",\"")
			metawr.WriteString(MetaHardlinksField)
			metawr.WriteString("\":[")
		}
		metasep := "\n\""

		iobuf := buffer.New(BufferSize)
		z, _ := getArchiveWriter(bd.compressionFormat, bd.compressionLevel)
		if ferr = z.Create(w); ferr != nil {
			return
		}
		defer z.Close()
		if ferr = filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			bar.Add64(info.Size())
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer file.Close()
			relativePath := strings.TrimPrefix(strings.TrimPrefix(filePath, localPath), "/")
			if diffFromPath != "" {
				diffFromFile, err := os.Stat(filepath.Join(diffFromPath, relativePath))
				if err == nil {
					if os.SameFile(info, diffFromFile) {
						metawr.WriteString(metasep)
						writeJsonString(metawr, relativePath)
						metawr.WriteByte('"')
						metasep = ",\n\""
						return nil
					}
				}
			}
			bfile := nio.NewReader(file, iobuf)
			defer bfile.Close()
			return z.Write(archiver.File{
				FileInfo: archiver.FileInfo{
					FileInfo:   info,
					CustomName: relativePath,
				},
				ReadCloser: bfile,
			})
		}); ferr != nil {
			return
		}

		if diffFromPath != "" {
			metawr.WriteString("]}")
			metawr.Flush()
			mf.Seek(0, 0)
			info, err := mf.Stat()
			if err != nil {
				ferr = fmt.Errorf("can't get stat: %v", err)
				return
			}
			if err := z.Write(archiver.File{
				FileInfo: archiver.FileInfo{
					FileInfo:   info,
					CustomName: MetaFileName,
				},
				ReadCloser: mf,
			}); err != nil {
				ferr = fmt.Errorf("can't add mata.json to archive: %v", err)
				return
			}
		}
		return
	}()

	if err := bd.PutFile(archiveName, body); err != nil {
		return err
	}
	bar.Finish()
	return nil
}

func NewBackupDestination(config Config) (*BackupDestination, error) {
	switch config.RemoteStorage {
    case "dir":
        dir := &Dir{Config: &config.Dir}
		return &BackupDestination{
			dir,
			config.Dir.Path,
			config.Dir.CompressionFormat,
			config.Dir.CompressionLevel,
			config.DisableProgressBar,
			config.BackupsToKeepRemote,
		}, nil
	case "azblob":
		azblob := &AzureBlob{Config: &config.AzureBlob}
		return &BackupDestination{
			azblob,
			config.AzureBlob.Path,
			config.AzureBlob.CompressionFormat,
			config.AzureBlob.CompressionLevel,
			config.DisableProgressBar,
			config.BackupsToKeepRemote,
		}, nil
	case "s3":
		s3 := &S3{Config: &config.S3}
		return &BackupDestination{
			s3,
			config.S3.Path,
			config.S3.CompressionFormat,
			config.S3.CompressionLevel,
			config.DisableProgressBar,
			config.BackupsToKeepRemote,
		}, nil
	case "gcs":
		gcs := &GCS{Config: &config.GCS}
		return &BackupDestination{
			gcs,
			config.GCS.Path,
			config.GCS.CompressionFormat,
			config.GCS.CompressionLevel,
			config.DisableProgressBar,
			config.BackupsToKeepRemote,
		}, nil
	case "cos":
		cos := &COS{Config: &config.COS}
		return &BackupDestination{
			cos,
			config.COS.Path,
			config.COS.CompressionFormat,
			config.COS.CompressionLevel,
			config.DisableProgressBar,
			config.BackupsToKeepRemote,
		}, nil
	case "ftp":
		ftp := &FTP{Config: &config.FTP}
		return &BackupDestination{
			ftp,
			config.FTP.Path,
			config.FTP.CompressionFormat,
			config.FTP.CompressionLevel,
			config.DisableProgressBar,
			config.BackupsToKeepRemote,
		}, nil
	default:
		return nil, fmt.Errorf("storage type '%s' not supported", config.RemoteStorage)
	}
}
