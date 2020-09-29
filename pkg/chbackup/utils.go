package chbackup

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mholt/archiver"
)

type Backup struct {
	Name string
	Size int64
	Date time.Time
	Full bool
}

func cleanDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func isClickhouseShadow(path string) bool {
	d, err := os.Open(path)
	if err != nil {
		return false
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return false
	}
	for _, name := range names {
		if name == "increment.txt" {
			continue
		}
		if _, err := strconv.Atoi(name); err != nil {
			return false
		}
	}
	return true
}

func moveShadow(shadowPath, backupPath string) error {
	if err := filepath.Walk(shadowPath, func(filePath string, info os.FileInfo, err error) error {
		relativePath := strings.Trim(strings.TrimPrefix(filePath, shadowPath), "/")
		pathParts := strings.SplitN(relativePath, "/", 3)
		if len(pathParts) != 3 {
			return nil
		}
		dstFilePath := filepath.Join(backupPath, pathParts[2])
		if info.IsDir() {
			return os.MkdirAll(dstFilePath, os.ModePerm)
		}
		if !info.Mode().IsRegular() {
			log.Printf("'%s' is not a regular file, skipping", filePath)
			return nil
		}
		return os.Rename(filePath, dstFilePath)
	}); err != nil {
		return err
	}
	return cleanDir(shadowPath)
}

func copyFile(srcFile string, dstFile string) error {
	if err := os.MkdirAll(path.Dir(dstFile), os.ModePerm); err != nil {
		return err
	}
	src, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(dstFile)
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	return err
}

func prepareBackupDir(config Config, dataPath, backupName, tmpName string, cleanShadow bool) (backupPath, backupDest string, err error) {
	backupPath = path.Join(dataPath, "backup.tmp", tmpName)
	backupDest = path.Join(dataPath, "backup", backupName)
	if err = os.MkdirAll(path.Dir(backupDest), os.ModePerm); err != nil {
		return
	}
	if _, err = os.Stat(backupDest); err == nil || !os.IsNotExist(err) {
		err = os.ErrExist
		return
	}
	if config.AutoCleanMaxRetry != 0 {
		shadowDir := path.Join(dataPath, "shadow")
		for i := config.AutoCleanMaxRetry; ; i-- {
			ok := true

			if _, err = os.Stat(backupPath); os.IsNotExist(err) {
				// nothing to clean
			} else if err = os.RemoveAll(backupPath); err == nil {
				log.Printf("Cleaned up incomplete backup %s.\n", backupPath)
			} else if !os.IsNotExist(err) {
				ok = false
			}

			if !cleanShadow {
				// don't need to clean shadow
			} else if _, err = os.Stat(shadowDir); os.IsNotExist(err) {
				// nothing to clean
			} else if err = os.RemoveAll(shadowDir); err == nil {
				log.Printf("Cleaned up shadow directory. Waiting for straggling freeze operations to finish before rechecking...\n")
				time.Sleep(config.AutoCleanDelay)
				ok = false
			} else if !os.IsNotExist(err) {
				ok = false
			}

			if ok {
				break
			} else if i == 0 {
				err = fmt.Errorf("can't create backup: auto-clean failed")
				return
			}
		}
	}
	err = os.MkdirAll(backupPath, os.ModePerm)
	return
}

func GetBackupsToDelete(backups []Backup, keep int) []Backup {
	if len(backups) > keep {
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].Date.After(backups[j].Date)
		})
		return backups[keep:]
	}
	return []Backup{}
}

func getArchiveWriter(format string, level int) (archiver.Writer, error) {
	switch format {
	case "tar":
		return &archiver.Tar{}, nil
	case "lz4":
		return &archiver.TarLz4{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "bzip2":
		return &archiver.TarBz2{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "gzip":
		return &archiver.TarGz{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "sz":
		return &archiver.TarSz{Tar: archiver.NewTar()}, nil
	case "xz":
		return &archiver.TarXz{Tar: archiver.NewTar()}, nil
	case "zstd":
		return &archiver.TarZstd{Tar: archiver.NewTar()}, nil
	}
	return nil, fmt.Errorf("wrong compression_format, supported: 'lz4', 'bzip2', 'gzip', 'sz', 'xz', 'zstd'")
}

func getExtension(format string) string {
	switch format {
	case "tar":
		return "tar"
	case "lz4":
		return "tar.lz4"
	case "bzip2":
		return "tar.bz2"
	case "gzip":
		return "tar.gz"
	case "sz":
		return "tar.sz"
	case "xz":
		return "tar.xz"
	case "zstd":
		return "tar.zst"
	}
	return ""
}

func getArchiveFormat(s string) (string, string) {
	switch {
	case strings.HasSuffix(s, ".tar"):     return "tar",   "tar"
	case strings.HasSuffix(s, ".tar.lz4"): return "lz4",   "tar.lz4"
	case strings.HasSuffix(s, ".tar.bz2"): return "bzip2", "tar.bz2"
	case strings.HasSuffix(s, ".tar.gz"):  return "gzip",  "tar.gz"
	case strings.HasSuffix(s, ".tar.sz"):  return "sz",    "tar.sz"
	case strings.HasSuffix(s, ".tar.xz"):  return "xz",    "tar.xz"
	case strings.HasSuffix(s, ".tar.zst"): return "zstd",  "tar.zst"
	default: return "", ""
	}
}

func isArchiveExtension(s string) bool {
	format, _ := getArchiveFormat(s)
	return format != ""
}

func stripArchiveExtension(s string) string {
	if _, extension := getArchiveFormat(s); extension != "" {
		return s[:len(s) - len(extension) - 1]
	} else {
		return ""
	}
}

func getArchiveReader(format string) (archiver.Reader, error) {
	switch format {
	case "tar":
		return archiver.NewTar(), nil
	case "lz4":
		return archiver.NewTarLz4(), nil
	case "bzip2":
		return archiver.NewTarBz2(), nil
	case "gzip":
		return archiver.NewTarGz(), nil
	case "sz":
		return archiver.NewTarSz(), nil
	case "xz":
		return archiver.NewTarXz(), nil
	case "zstd":
		return archiver.NewTarZstd(), nil
	}
	return nil, fmt.Errorf("wrong compression_format, supported: 'tar', 'lz4', 'bzip2', 'gzip', 'sz', 'xz', 'zstd'")
}

// FormatBytes - Convert bytes to human readable string
func FormatBytes(i int64) (result string) {
	const (
		KiB = 1024
		MiB = 1048576
		GiB = 1073741824
		TiB = 1099511627776
	)
	switch {
	case i >= TiB:
		result = fmt.Sprintf("%.02f TiB", float64(i)/TiB)
	case i >= GiB:
		result = fmt.Sprintf("%.02f GiB", float64(i)/GiB)
	case i >= MiB:
		result = fmt.Sprintf("%.02f MiB", float64(i)/MiB)
	case i >= KiB:
		result = fmt.Sprintf("%.02f KiB", float64(i)/KiB)
	default:
		result = fmt.Sprintf("%d B", i)
	}
	return
}

func TablePathEncode(str string) string {
	return strings.ReplaceAll(url.PathEscape(str), ".", "%2E")
}

func parseTime(text string) (t time.Time, err error) {
	timeFormats := []string{
		"Mon, 02 Jan 2006 15:04:05 GMT",
		time.RFC850,
		time.ANSIC,
		time.RFC3339,
	}

	for _, layout := range timeFormats {
		t, err = time.Parse(layout, text)
		if err == nil {
			return
		}
	}
	return
}

func writeError(w http.ResponseWriter, statusCode int, operation string, err error) {
	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	out, _ := json.Marshal(struct {
		Status    string `json:"status"`
		Operation string `json:"operation,omitempty"`
		Error     string `json:"error"`
	}{
		Status:    "error",
		Operation: operation,
		Error:     err.Error(),
	})
	fmt.Fprintln(w, string(out))
}

func sendResponse(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(statusCode)
	out, _ := json.Marshal(&v)
	fmt.Fprintln(w, string(out))
}

func writeJsonString(w *bufio.Writer, s string) {
	const hex string = "0123456789abcdef"
	l := len(s)
	for i := 0; i < l; i++ {
		c := s[i]
		if c >= 0x20 && c != '\\' && c != '"' {
			// NB: this works because valid UTF-8 encodings of more than 1 byte have MSB set
			w.WriteByte(c)
			continue
		}
		w.WriteByte('\\')
		switch c {
		case '\\', '"':
			w.WriteByte(c)
		case '\n':
			w.WriteByte('n')
		case '\f':
			w.WriteByte('f')
		case '\b':
			w.WriteByte('b')
		case '\r':
			w.WriteByte('r')
		case '\t':
			w.WriteByte('t')
		default:
			w.WriteString("u00")
			w.WriteByte(hex[c>>4])
			w.WriteByte(hex[c&0xF])
		}
	}
}
