package chbackup

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	// ErrUnknownClickhouseDataPath -
	ErrUnknownClickhouseDataPath = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

func addTable(tables []Table, table Table) []Table {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
			return tables
		}
	}
	return append(tables, table)
}

func addBackupTable(tables BackupTables, table BackupTable) BackupTables {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Name == table.Name) {
			return tables
		}
	}
	return append(tables, table)
}

func addRestoreTable(tables RestoreTables, table RestoreTable) RestoreTables {
	for _, t := range tables {
		if (t.Database == table.Database) && (t.Table == table.Table) {
			return tables
		}
	}
	return append(tables, table)
}

func parseTablePatternForFreeze(tables []Table, tablePattern string) []Table {
	if tablePattern == "" {
		return tables
	}
	tablePatterns := strings.Split(tablePattern, ",")
	var result []Table
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			if matched, _ := filepath.Match(pattern, fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				result = addTable(result, t)
			}
		}
	}
	return result
}

func parseTablePatternForRestoreData(tables map[string]BackupTable, tablePattern string) []BackupTable {
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	result := BackupTables{}
	for _, t := range tables {
		for _, pattern := range tablePatterns {
			tableName := fmt.Sprintf("%s.%s", t.Database, t.Name)
			if matched, _ := filepath.Match(pattern, tableName); matched {
				result = addBackupTable(result, t)
			}
		}
	}
	result.Sort()
	return result
}

func parseSchemaPattern(metadataPath string, tablePattern string) (RestoreTables, error) {
	regularTables := RestoreTables{}
	distributedTables := RestoreTables{}
	viewTables := RestoreTables{}
	tablePatterns := []string{"*"}
	if tablePattern != "" {
		tablePatterns = strings.Split(tablePattern, ",")
	}
	if err := filepath.Walk(metadataPath, func(filePath string, info os.FileInfo, err error) error {
		if !strings.HasSuffix(filePath, ".sql") || !info.Mode().IsRegular() {
			return nil
		}
		p := filepath.ToSlash(filePath)
		p = strings.Trim(strings.TrimPrefix(strings.TrimSuffix(p, ".sql"), metadataPath), "/")
		parts := strings.Split(p, "/")
		if len(parts) != 2 {
			return nil
		}
		database, _ := url.PathUnescape(parts[0])
		table, _ := url.PathUnescape(parts[1])
		tableName := fmt.Sprintf("%s.%s", database, table)
		for _, p := range tablePatterns {
			if matched, _ := filepath.Match(p, tableName); matched {
				data, err := ioutil.ReadFile(filePath)
				if err != nil {
					return err
				}
				restoreTable := RestoreTable{
					Database: database,
					Table:    table,
					Query:    strings.Replace(string(data), "ATTACH", "CREATE", 1),
					Path:     filePath,
				}
				if strings.Contains(restoreTable.Query, "ENGINE = Distributed") {
					distributedTables = addRestoreTable(distributedTables, restoreTable)
					return nil
				}
				if strings.HasPrefix(restoreTable.Query, "CREATE VIEW") ||
					strings.HasPrefix(restoreTable.Query, "CREATE MATERIALIZED VIEW") {
					viewTables = addRestoreTable(viewTables, restoreTable)
					return nil
				}
				regularTables = addRestoreTable(regularTables, restoreTable)
				return nil
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	regularTables.Sort()
	distributedTables.Sort()
	viewTables.Sort()
	result := append(regularTables, distributedTables...)
	result = append(result, viewTables...)
	return result, nil
}

// getTables - get all tables for use by PrintTables and API
func getTables(config Config) ([]Table, error) {
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}

	if err := ch.Connect(); err != nil {
		return []Table{}, fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	allTables, err := ch.GetTables()
	if err != nil {
		return []Table{}, fmt.Errorf("can't get tables: %v", err)
	}
	return allTables, nil
}

// PrintTables - print all tables suitable for backup
func PrintTables(config Config) error {
	allTables, err := getTables(config)
	if err != nil {
		return err
	}
	for _, table := range allTables {
		if table.Skip {
			fmt.Printf("%s.%s\t(ignored)\n", table.Database, table.Name)
		} else {
			fmt.Printf("%s.%s\n", table.Database, table.Name)
		}
	}
	return nil
}

func restoreSchema(config Config, backupName string, tablePattern string) error {
	if backupName == "" {
		PrintLocalBackups(config, "all")
		return fmt.Errorf("select backup for restore")
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	metadataPath := path.Join(dataPath, "backup", backupName, "metadata")
	info, err := os.Stat(metadataPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a dir", metadataPath)
	}
	tablesForRestore, err := parseSchemaPattern(metadataPath, tablePattern)
	if err != nil {
		return err
	}
	if len(tablesForRestore) == 0 {
		return fmt.Errorf("no have found schemas by %s in %s", tablePattern, backupName)
	}
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	for _, schema := range tablesForRestore {
		if err := ch.CreateDatabase(schema.Database); err != nil {
			return fmt.Errorf("can't create database '%s': %v", schema.Database, err)
		}
		if err := ch.CreateTable(schema); err != nil {
			return fmt.Errorf("can't create table '%s.%s': %v", schema.Database, schema.Table, err)
		}
	}
	return nil
}

func printBackups(backupList []Backup, format string, printSize bool) error {
	switch format {
	case "latest", "last", "l":
		if len(backupList) < 1 {
			return fmt.Errorf("no backups found")
		}
		fmt.Println(backupList[len(backupList)-1].Name)
	case "penult", "prev", "previous", "p":
		if len(backupList) < 2 {
			return fmt.Errorf("no penult backup is found")
		}
		fmt.Println(backupList[len(backupList)-2].Name)
	case "all", "":
		if len(backupList) == 0 {
			fmt.Println("no backups found")
		}
		for _, backup := range backupList {
			if printSize {
				fmt.Printf("- '%s'\t%s\t(created at %s)\n", backup.Name, FormatBytes(backup.Size), backup.Date.Format("02-01-2006 15:04:05"))
			} else {
				fmt.Printf("- '%s'\t(created at %s)\n", backup.Name, backup.Date.Format("02-01-2006 15:04:05"))
			}
		}
	default:
		return fmt.Errorf("'%s' undefined", format)
	}
	return nil
}

// PrintLocalBackups - print all backups stored locally
func PrintLocalBackups(config Config, format string) error {
	backupList, err := ListLocalBackups(config)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return printBackups(backupList, format, false)
}

// ListLocalBackups - return slice of all backups stored locally
func ListLocalBackups(config Config) ([]Backup, error) {
	dataPath := getDataPath(config)
	if dataPath == "" {
		return nil, ErrUnknownClickhouseDataPath
	}
	backupsPath := path.Join(dataPath, "backup")
	result := []Backup{}
	d, err := os.Open(backupsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return nil, err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		info, err := os.Stat(path.Join(backupsPath, name))
		if err != nil {
			continue
		}
		if !info.IsDir() {
			continue
		}
		result = append(result, Backup{
			Name: name,
			Date: info.ModTime(),
		})
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Date.Before(result[j].Date)
	})
	return result, nil
}

// getRemoteBackups - get all backups stored on remote storage
func getRemoteBackups(config Config) ([]Backup, error) {
	if config.RemoteStorage == "none" {
		fmt.Println("PrintRemoteBackups aborted: RemoteStorage set to \"none\"")
		return []Backup{}, nil
	}
	bd, err := NewBackupDestination(config)
	if err != nil {
		return []Backup{}, err
	}
	err = bd.Connect()
	if err != nil {
		return []Backup{}, err
	}

	backupList, err := bd.BackupList()
	if err != nil {
		return []Backup{}, err
	}
	return backupList, err
}

// PrintRemoteBackups - print all backups stored on remote storage
func PrintRemoteBackups(config Config, format string) error {
	backupList, err := getRemoteBackups(config)
	if err != nil {
		return err
	}
	return printBackups(backupList, format, true)
}

// Freeze - freeze tables by tablePattern
func Freeze(config Config, tablePattern string) error {
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	dataPath, err := ch.GetDataPath()
	if err != nil || dataPath == "" {
		return fmt.Errorf("can't get data path from clickhouse: %v\nyou can set data_path in config file", err)
	}

	shadowPath := filepath.Join(dataPath, "shadow")
	files, err := ioutil.ReadDir(shadowPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("can't read %s directory: %v", shadowPath, err)
		}
	} else if len(files) > 0 {
		return fmt.Errorf("'%s' is not empty, execute 'clean' command first", shadowPath)
	}

	allTables, err := ch.GetTables()
	if err != nil {
		return fmt.Errorf("can't get tables from clickhouse: %v", err)
	}
	backupTables := parseTablePatternForFreeze(allTables, tablePattern)
	if len(backupTables) == 0 {
		return fmt.Errorf("there are no tables in clickhouse, create something to freeze")
	}
	for _, table := range backupTables {
		if table.Skip {
			log.Printf("Skip '%s.%s'", table.Database, table.Name)
			continue
		}
		if err := ch.FreezeTable(table); err != nil {
			return err
		}
	}
	return nil
}

// NewBackupName - return default backup name
func NewBackupName(config Config) string {
	return time.Now().UTC().Format(config.BackupNameFormat)
}

// CreateBackup - create new backup of all tables matched by tablePattern
// If backupName is empty string will use default backup name
func CreateBackup(config Config, backupName, tablePattern string) error {
	if backupName == "" {
		if backupName = NewBackupName(config); backupName == "" {
			return fmt.Errorf("backup name is not specified and BackupNameFormat is not set")
		}
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupPath, backupDest, err := prepareBackupDir(config, dataPath, backupName, "create", true)
	if os.IsExist(err) {
		return fmt.Errorf("backup '%s' already exists", backupName)
	} else if err != nil {
		return fmt.Errorf("can't create backup: %v", err)
	}
	log.Printf("Create backup '%s'", backupName)
	if err := Freeze(config, tablePattern); err != nil {
		return err
	}
	log.Println("Copy metadata")
	schemaList, err := parseSchemaPattern(path.Join(dataPath, "metadata"), tablePattern)
	if err != nil {
		return err
	}
	for _, schema := range schemaList {
		skip := false
		for _, filter := range config.ClickHouse.SkipTables {
			if matched, _ := filepath.Match(filter, fmt.Sprintf("%s.%s", schema.Database, schema.Table)); matched {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		relativePath := strings.Trim(strings.TrimPrefix(schema.Path, path.Join(dataPath, "metadata")), "/")
		newPath := path.Join(backupPath, "metadata", relativePath)
		if err := copyFile(schema.Path, newPath); err != nil {
			return fmt.Errorf("can't backup metadata: %v", err)
		}
	}
	log.Println("  Done.")

	log.Println("Move shadow")
	backupShadowDir := path.Join(backupPath, "shadow")
	if err := os.MkdirAll(backupShadowDir, os.ModePerm); err != nil {
		return err
	}
	shadowDir := path.Join(dataPath, "shadow")
	if err := moveShadow(shadowDir, backupShadowDir); err != nil {
		return err
	}
	if err := os.Rename(backupPath, backupDest); err != nil {
		return fmt.Errorf("cannot commit backup: %v", err)
	}
	if err := RemoveOldBackupsLocal(config); err != nil {
		return err
	}
	log.Println("  Done.")
	return nil
}

// Restore - restore tables matched by tablePattern from backupName
func Restore(config Config, backupName string, tablePattern string, schemaOnly bool, dataOnly bool) error {
	if schemaOnly || (schemaOnly == dataOnly) {
		err := restoreSchema(config, backupName, tablePattern)
		if err != nil {
			return err
		}
	}
	if dataOnly || (schemaOnly == dataOnly) {
		err := RestoreData(config, backupName, tablePattern)
		if err != nil {
			return err
		}
	}
	return nil
}

// RestoreData - restore data for tables matched by tablePattern from backupName
func RestoreData(config Config, backupName string, tablePattern string) error {
	if backupName == "" {
		PrintLocalBackups(config, "all")
		return fmt.Errorf("select backup for restore")
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	ch := &ClickHouse{
		Config: &config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()

	allBackupTables, err := ch.GetBackupTables(backupName)
	if err != nil {
		return err
	}
	restoreTables := parseTablePatternForRestoreData(allBackupTables, tablePattern)
	chTables, err := ch.GetTables()
	if err != nil {
		return err
	}
	if len(restoreTables) == 0 {
		return fmt.Errorf("backup doesn't have tables to restore")
	}
	missingTables := []string{}
	for _, restoreTable := range restoreTables {
		found := false
		for _, chTable := range chTables {
			if (restoreTable.Database == chTable.Database) && (restoreTable.Name == chTable.Name) {
				found = true
				break
			}
		}
		if !found {
			missingTables = append(missingTables, fmt.Sprintf("'%s.%s'", restoreTable.Database, restoreTable.Name))
		}
	}
	if len(missingTables) > 0 {
		return fmt.Errorf("%s is not created. Restore schema first or create missing tables manually", strings.Join(missingTables, ", "))
	}
	for _, table := range restoreTables {
		if err := ch.CopyData(table); err != nil {
			return fmt.Errorf("can't restore '%s.%s': %v", table.Database, table.Name, err)
		}
		if err := ch.AttachPatritions(table); err != nil {
			return fmt.Errorf("can't attach partitions for table '%s.%s': %v", table.Database, table.Name, err)
		}
	}
	return nil
}

func getDataPath(config Config) string {
	if config.ClickHouse.DataPath != "" {
		return config.ClickHouse.DataPath
	}
	ch := &ClickHouse{Config: &config.ClickHouse}
	if err := ch.Connect(); err != nil {
		return ""
	}
	defer ch.Close()
	dataPath, err := ch.GetDataPath()
	if err != nil {
		return ""
	}
	return dataPath
}

func GetLocalBackup(config Config, backupName string) error {
	if backupName == "" {
		return fmt.Errorf("backup name is required")
	}
	backupList, err := ListLocalBackups(config)
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return nil
		}
	}
	return fmt.Errorf("backup '%s' not found", backupName)
}

type backupPair struct {
	Local  int
	LDate  time.Time
	Remote int
	RDate  time.Time
}

func doSync(config Config, dryRun bool,
	do func(
		dataPath string,
		bd       *BackupDestination,
		backupListLocal   []Backup,
		backupListRemote  []Backup,
		addBackupToDelete func(Backup),
		previousBackup    backupPair,
		pair             *backupPair,
	) error,
	apply func(
		dataPath string,
		bd       *BackupDestination,
		backupListLocal   []Backup,
		backupListRemote  []Backup,
		addBackupToDelete func(Backup),
	) error) error {

	if config.RemoteStorage == "none" {
		return fmt.Errorf("Sync aborted: RemoteStorage set to \"none\"")
	}
	if config.FullBackupInterval == 0 {
		return fmt.Errorf("Sync aborted: FullBackupInterval is not set")
	}
	if config.BackupNameFormat == "" {
		return fmt.Errorf("Sync aborted: BackupNameFormat is not set")
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupListLocal, err := ListLocalBackups(config)
	if err != nil {
		return err
	}

	bd, err := NewBackupDestination(config)
	if err != nil {
		return err
	}

	err = bd.Connect()
	if err != nil {
		return fmt.Errorf("can't connect to %s: %v", bd.Kind(), err)
	}

	backupListRemote, err := bd.BackupList()
	if err != nil {
		return err
	}

	// NOTE: Sync requires backup names to be date-based
	// I don't need dates reported by storage locations
	for i, _ := range backupListLocal {
		name := backupListLocal[i].Name
		backupListLocal[i].Date, _ = time.Parse(config.BackupNameFormat, name)
	}
	for i, _ := range backupListRemote {
		name := stripArchiveExtension(backupListRemote[i].Name)
		full := false
		if strings.HasSuffix(name, "-full") {
			name = name[:len(name) - 5]
			full = true
		}
		backupListRemote[i].Date, _ = time.Parse(config.BackupNameFormat, name)
		backupListRemote[i].Full    = full
	}
	sort.SliceStable(backupListLocal,  func(i, j int) bool {
		return backupListLocal[i].Date.Before(backupListLocal[j].Date)
	})
	sort.SliceStable(backupListRemote, func(i, j int) bool {
		// sort full backups before matching difference backups
		// to be able to skip the latter below and avoid creating extra full backups
		return backupListRemote[i].Date.Before(backupListRemote[j].Date) ||
			backupListRemote[i].Date == backupListRemote[j].Date &&
			backupListRemote[i].Full &&!backupListRemote[j].Full
	})

	var previousBackup     backupPair
	var backupsToDelete    []Backup

	mapBackupsToDelete := make(map[string]struct{})
	addBackupToDelete  := func(backup Backup) {
		if _, ok := mapBackupsToDelete[backup.Name]; !ok {
			mapBackupsToDelete[backup.Name] = struct{}{}
			backupsToDelete = append(backupsToDelete, backup)
		}
	}

	il := 0
	ir := 0
	for {
		var dl, dr time.Time
		if il != len(backupListLocal) {
			dl = backupListLocal[il].Date
			if dl.IsZero() {
				log.Printf("Skipping unexpected local backup %s\n", backupListLocal[il].Name)
				il++
				continue
			}
		}
		if ir != len(backupListRemote) {
			dr = backupListRemote[ir].Date
			if dr.IsZero() {
				log.Printf("Skipping unexpected remote backup %s\n", backupListRemote[ir].Name)
				ir++
				continue
			}
		}
		if dl.IsZero() && dr.IsZero() {
			break
		}

		var pair backupPair
		switch {
		case dl == dr:
			pair.Local  = il
			pair.LDate  = dl
			pair.Remote = ir
			pair.RDate  = dr
			il++
			ir++
		case !dl.IsZero() && dl.Before(dr) || dr.IsZero():
			pair.Local  = il
			pair.LDate  = dl
			il++
		default:
			pair.Remote = ir
			pair.RDate  = dr
			ir++
		}

		log.Printf("%+v\n", pair)

		if err = do(dataPath, bd, backupListLocal, backupListRemote, addBackupToDelete, previousBackup, &pair); err != nil {
			return err
		}

		previousBackup = pair
	}

	if apply != nil {
		if err = apply(dataPath, bd, backupListLocal, backupListRemote, addBackupToDelete); err != nil {
			return err
		}
	}

	sort.SliceStable(backupsToDelete, func(i, j int) bool {
		return backupsToDelete[i].Date.Before(backupsToDelete[j].Date)
	})

	if i := len(backupsToDelete) - config.BackupsToKeepLocal; i > 0 {
		for _, b := range backupsToDelete[:i] {
			log.Println("Deleting old local backup " + b.Name)

			if !dryRun {
				os.RemoveAll(path.Join(dataPath, "backup", b.Name))
			}
		}
	}

	log.Println("  Done.")
	return nil
}

func UploadSync(config Config, dryRun bool) error {
	var lastFullBackupDate time.Time
	return doSync(config, dryRun, func (
		dataPath string,
		bd       *BackupDestination,
		backupListLocal   []Backup,
		backupListRemote  []Backup,
		addBackupToDelete func(Backup),
		previousBackup    backupPair,
		pair             *backupPair,
	) error {
		var err error
		var bl  Backup
		var br  Backup
		dl := pair.LDate
		dr := pair.RDate

		if !dl.IsZero() {
			bl = backupListLocal[pair.Local]
		}
		if !dr.IsZero() {
			br = backupListRemote[pair.Remote]
		}

		if !dr.IsZero() && br.Full {
			lastFullBackupDate = dr
		}

		if !dl.IsZero() {
			localName := bl.Name
			diffFrom  := ""
			alsoFull  := false

			// always upload the difference backup for continuity purposes
			if dr.IsZero() && !previousBackup.LDate.IsZero() {
				diffFrom = backupListLocal[previousBackup.Local].Name
				log.Printf("Uploading %s difference from %s\n", localName, diffFrom)

				if !dryRun {
					if err = bd.CompressedStreamUpload(path.Join(dataPath, "backup", localName), localName, path.Join(dataPath, "backup", diffFrom)); err != nil {
						return fmt.Errorf("can't upload: %v", err)
					}
				}
			}

			if dr.IsZero() || !br.Full {
				if lastFullBackupDate.IsZero() || dl.Sub(lastFullBackupDate) >= config.FullBackupInterval {
					alsoFull = true
					log.Printf("Uploading %s-full\n", localName)
				} else if dr.IsZero() && diffFrom == "" {
					alsoFull = true
					log.Printf("Uploading %s-full because preceding local backup matching %s is missing\n", localName, backupListRemote[previousBackup.Remote].Name)
				}
			}

			if alsoFull {
				if !dryRun {
					if err = bd.CompressedStreamUpload(path.Join(dataPath, "backup", localName), localName + "-full", ""); err != nil {
						return fmt.Errorf("can't upload: %v", err)
					}
				}

				lastFullBackupDate = dl
			}

			// remember that we've uploaded this backup
			// .Remote will not be not used b/c local backup exists
			pair.RDate = dl
		}

		if !pair.RDate.IsZero() && !dl.IsZero() {
			addBackupToDelete(bl)
		}

		return nil
	}, nil)
}

func Upload(config Config, backupName string, diffFrom string) error {
	if config.RemoteStorage == "none" {
		fmt.Println("Upload aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		PrintLocalBackups(config, "all")
		return fmt.Errorf("select backup for upload")
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}

	bd, err := NewBackupDestination(config)
	if err != nil {
		return err
	}

	err = bd.Connect()
	if err != nil {
		return fmt.Errorf("can't connect to %s: %v", bd.Kind(), err)
	}

	if err := GetLocalBackup(config, backupName); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	backupPath := path.Join(dataPath, "backup", backupName)
	log.Printf("Upload backup '%s'", backupName)
	diffFromPath := ""
	if diffFrom != "" {
		diffFromPath = path.Join(dataPath, "backup", diffFrom)
	}
	if err := bd.CompressedStreamUpload(backupPath, backupName, diffFromPath); err != nil {
		return fmt.Errorf("can't upload: %v", err)
	}
	if err := bd.RemoveOldBackups(bd.BackupsToKeep()); err != nil {
		return fmt.Errorf("can't remove old backups: %v", err)
	}
	log.Println("  Done.")
	return nil
}

func DownloadSync(config Config, dryRun bool) error {
	const MAX = int64(1 << 62)
	sizeSinceFull  := MAX
	sizeSinceLocal := MAX
	var downloadFull  []Backup
	var downloadLocal []Backup
	var ilLatest int
	return doSync(config, dryRun, func (
		dataPath string,
		bd       *BackupDestination,
		backupListLocal   []Backup,
		backupListRemote  []Backup,
		addBackupToDelete func(Backup),
		previousBackup    backupPair,
		pair             *backupPair,
	) error {
		dl := pair.LDate
		dr := pair.RDate

		if !dl.IsZero() && dr.IsZero() {
			log.Printf("Skipping unpaired local backup %s\n", backupListLocal[pair.Local].Name)
			return nil
		}

		var br Backup
		if !dr.IsZero() {
			br = backupListRemote[pair.Remote]
		}

		if br.Full {
			sizeSinceFull   = 0
			downloadFull    = nil
		}
		if sizeSinceFull != MAX {
			sizeSinceFull  += br.Size
			downloadFull    = append(downloadFull, br)
		}

		if !dl.IsZero() {
			sizeSinceLocal  = 0
			downloadLocal   = nil
			ilLatest        = pair.Local
		} else if sizeSinceLocal == MAX {
			return nil
		} else if !br.Full {
			sizeSinceLocal += br.Size
			downloadLocal   = append(downloadLocal, br)
		} else if pair.Remote != len(backupListRemote) - 1 && backupListRemote[pair.Remote + 1].Date != dr {
			// br is a full backup, but there is no matching difference backup for it
			// reset the local list at this juncture
			sizeSinceLocal  = MAX
			downloadLocal   = nil
		}

		if !dr.IsZero() && !dl.IsZero() {
			addBackupToDelete(backupListLocal[pair.Local])
		}

		return nil
	}, func (
		dataPath string,
		bd       *BackupDestination,
		backupListLocal   []Backup,
		backupListRemote  []Backup,
		addBackupToDelete func(Backup),
	) error {
		if sizeSinceFull == MAX && sizeSinceLocal == MAX {
			return fmt.Errorf("No matching local or remote backups exist")
		}
		if sizeSinceLocal == 0 {
			log.Printf("Up to date.\n")
			return nil
		}
		var list []Backup
		if sizeSinceLocal < sizeSinceFull {
			log.Printf("Download %d bytes of diffs following local backup %s\n", sizeSinceLocal, backupListLocal[ilLatest].Name)
			list = downloadLocal
		} else {
			log.Printf("Download %d bytes starting from remote backup %s\n", sizeSinceFull, downloadFull[0].Name)
			list = downloadFull
		}

		for _, backup := range list {
			backupName := backup.Name
			localName  := backup.Date.Format(config.BackupNameFormat)

			log.Printf("Downloading %s to %s...\n", backupName, localName)
			if !dryRun {
				backupTmp, backupDest, err := prepareBackupDir(config, dataPath, localName, backupName, false)
				if os.IsExist(err) {
					log.Printf("%s suddenly exists?!\n", localName)
					continue
				} else if err != nil {
					return fmt.Errorf("can't create backup: %v", err)
				}

				if err := bd.CompressedStreamDownload(backupName, backupTmp, func(requiredBackupName string) (string, error) {
					localPath := path.Join(dataPath, "backup", requiredBackupName)
					if s, err := os.Stat(localPath); err != nil {
						return "", err
					} else if !s.IsDir() {
						return "", fmt.Errorf("Not a directory")
					}

					// if the download finishes successfully, it will no longer be necessary to keep this backup locally
					d, _ := time.Parse(config.BackupNameFormat, requiredBackupName)
					addBackupToDelete(Backup{Name: requiredBackupName, Date: d})
					return localPath, os.ErrExist
				}); err != nil {
					os.RemoveAll(backupTmp)
					return err
				}

				if err := os.Rename(backupTmp, backupDest); err != nil {
					return fmt.Errorf("cannot commit backup: %v", err)
				}
			}
		}
		return nil
	})
}

func Download(config Config, backupName string) error {
	if config.RemoteStorage == "none" {
		fmt.Println("Download aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		PrintRemoteBackups(config, "all")
		return fmt.Errorf("select backup for download")
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	bd, err := NewBackupDestination(config)
	if err != nil {
		return err
	}

	err = bd.Connect()
	if err != nil {
		return err
	}
	// use the old assignment trick to create a recursive closure
	var downloadOuter func(string) (string, error)
	downloadOuter = func(backupName string) (string, error) {
		localPath := path.Join(dataPath, "backup", backupName)
		return localPath, bd.CompressedStreamDownload(backupName, localPath, downloadOuter)
	}
	if _, err = downloadOuter(backupName); err != nil {
		return err
	}
	log.Println("  Done.")
	return nil
}

// Clean - removed all data in shadow folder
func Clean(config Config) error {
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	shadowDir := path.Join(dataPath, "shadow")
	if _, err := os.Stat(shadowDir); os.IsNotExist(err) {
		log.Printf("%s directory does not exist, nothing to do", shadowDir)
		return nil
	}
	log.Printf("Clean %s", shadowDir)
	if err := cleanDir(shadowDir); err != nil {
		return fmt.Errorf("can't clean '%s': %v", shadowDir, err)
	}
	return nil
}

//
func RemoveOldBackupsLocal(config Config) error {
	if config.BackupsToKeepLocal < 1 {
		return nil
	}
	backupList, err := ListLocalBackups(config)
	if err != nil {
		return err
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	backupsToDelete := GetBackupsToDelete(backupList, config.BackupsToKeepLocal)
	for _, backup := range backupsToDelete {
		backupPath := path.Join(dataPath, "backup", backup.Name)
		os.RemoveAll(backupPath)
	}
	return nil
}

func RemoveBackupLocal(config Config, backupName string) error {
	backupList, err := ListLocalBackups(config)
	if err != nil {
		return err
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return os.RemoveAll(path.Join(dataPath, "backup", backupName))
		}
	}
	return fmt.Errorf("backup '%s' not found", backupName)
}

func RemoveBackupRemote(config Config, backupName string) error {
	if config.RemoteStorage == "none" {
		fmt.Println("RemoveBackupRemote aborted: RemoteStorage set to \"none\"")
		return nil
	}
	dataPath := getDataPath(config)
	if dataPath == "" {
		return ErrUnknownClickhouseDataPath
	}

	bd, err := NewBackupDestination(config)
	if err != nil {
		return err
	}
	err = bd.Connect()
	if err != nil {
		return fmt.Errorf("can't connect to remote storage: %v", err)
	}
	backupList, err := bd.BackupList()
	if err != nil {
		return err
	}
	for _, backup := range backupList {
		if backup.Name == backupName {
			return bd.RemoveBackup(backupName)
		}
	}
	return fmt.Errorf("backup '%s' not found on remote storage", backupName)
}
