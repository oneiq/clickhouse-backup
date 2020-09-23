package chbackup

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/kelseyhightower/envconfig"
	yaml "gopkg.in/yaml.v2"
)

// Config - config file format
type Config struct {
	GeneralConfig               `yaml:"general"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse" split_words:"false"`
	S3         S3Config         `yaml:"s3"`
	GCS        GCSConfig        `yaml:"gcs"`
	COS        COSConfig        `yaml:"cos"`
	API        APIConfig        `yaml:"api"`
	FTP        FTPConfig        `yaml:"ftp"`
	AzureBlob  AzureBlobConfig  `yaml:"azblob" envconfig:"AZBLOB"`
	Dir        DirConfig        `yaml:"dir"`
}

// GeneralConfig - general setting section
type GeneralConfig struct {
	RemoteStorage       string `yaml:"remote_storage"`
	DisableProgressBar  bool   `yaml:"disable_progress_bar"`
	BackupsToKeepLocal  int    `yaml:"backups_to_keep_local"`
	BackupsToKeepRemote int    `yaml:"backups_to_keep_remote"`
	BackupNameFormat    string `yaml:"backup_name_format"`
	AutoCleanMaxRetry   int    `yaml:"auto_clean_max_retry"`
	AutoCleanDelay      time.Duration `yaml:"auto_clean_delay"`
}

type DestinationConfig struct {
	Path              string `yaml:"path"`
	CompressionLevel  int    `yaml:"compression_level"`
	CompressionFormat string `yaml:"compression_format"`
}

// GCSConfig - GCS settings section
type GCSConfig struct {
	DestinationConfig        `yaml:",inline"`
	CredentialsFile   string `yaml:"credentials_file"`
	CredentialsJSON   string `yaml:"credentials_json"`
	Bucket            string `yaml:"bucket"`
}

// DirConfig - directory settings section
type DirConfig struct {
	DestinationConfig `yaml:",inline"`
}

// AzureBlobConfig - Azure Blob settings section
type AzureBlobConfig struct {
	DestinationConfig                   `yaml:",inline"`
	EndpointSuffix        string        `yaml:"endpoint_suffix"`
	AccountName           string        `yaml:"account_name"`
	AccountKey            string        `yaml:"account_key"`
	SharedAccessSignature string        `yaml:"sas" envconfig:"AZBLOB_SAS"`
	Container             string        `yaml:"container"`
	DownloadMaxRequests   int           `yaml:"download_max_requests"`
	UploadMaxBuffers      int           `yaml:"upload_max_buffers"`
	UploadPartSize        int           `yaml:"upload_part_size"`
	MaxTries              int32         `yaml:"max_tries"`
	TryTimeout            time.Duration `yaml:"try_timeout"`
	SSEKey                string        `yaml:"sse_key"`
}

// S3Config - s3 settings section
type S3Config struct {
	DestinationConfig              `yaml:",inline"`
	AccessKey               string `yaml:"access_key"`
	SecretKey               string `yaml:"secret_key"`
	Bucket                  string `yaml:"bucket"`
	Endpoint                string `yaml:"endpoint"`
	Region                  string `yaml:"region"`
	ACL                     string `yaml:"acl"`
	ForcePathStyle          bool   `yaml:"force_path_style"`
	DisableSSL              bool   `yaml:"disable_ssl"`
	PartSize                int64  `yaml:"part_size"`
	SSE                     string `yaml:"sse"`
	DisableCertVerification bool   `yaml:"disable_cert_verification"`
	Debug                   bool   `yaml:"debug"`
}

// COSConfig - cos settings section
type COSConfig struct {
	DestinationConfig               `yaml:",inline"`
	RowURL            string        `yaml:"url" envconfig:"URL"`
	Timeout           time.Duration `yaml:"timeout"`
	SecretID          string        `yaml:"secret_id"`
	SecretKey         string        `yaml:"secret_key"`
	Debug             bool          `yaml:"debug"`
}

// FTPConfig - ftp settings section
type FTPConfig struct {
	DestinationConfig               `yaml:",inline"`
	Address           string        `yaml:"address"`
	Timeout           time.Duration `yaml:"timeout"`
	Username          string        `yaml:"username"`
	Password          string        `yaml:"password"`
	TLS               bool          `yaml:"tls"`
	Debug             bool          `yaml:"debug"`
}

// ClickHouseConfig - clickhouse settings section
type ClickHouseConfig struct {
	Username     string        `yaml:"username"`
	Password     string        `yaml:"password"`
	Host         string        `yaml:"host"`
	Port         uint          `yaml:"port"`
	DataPath     string        `yaml:"data_path"`
	SkipTables   []string      `yaml:"skip_tables"`
	Timeout      time.Duration `yaml:"timeout"`
	FreezeByPart bool          `yaml:"freeze_by_part"`
	FreezeRetry  int           `yaml:"freeze_retry"`
}

type APIConfig struct {
	ListenAddr    string `yaml:"listen" envconfig:"API_LISTEN"`
	EnableMetrics bool   `yaml:"enable_metrics"`
	EnablePprof   bool   `yaml:"enable_pprof"`
	Username      string `yaml:"username"`
	Password      string `yaml:"password"`
}

// LoadConfig - load config from file
func LoadConfig(configLocation string) (*Config, error) {
	config := DefaultConfig()
	configEnv := envconfig.EnvConfig{Spec: config, DefaultSplitWords: true}
	configYaml, err := ioutil.ReadFile(configLocation)
	if os.IsNotExist(err) {
		err := configEnv.Process()
		return config, err
	}
	if err != nil {
		return nil, fmt.Errorf("can't open config file: %v", err)
	}
	if err := yaml.Unmarshal(configYaml, &config); err != nil {
		return nil, fmt.Errorf("can't parse config file: %v", err)
	}
	if err := configEnv.Process(); err != nil {
		return nil, err
	}
	return config, validateConfig(config)
}

func validateConfig(config *Config) error {
	if _, err := getArchiveWriter(config.S3.CompressionFormat, config.S3.CompressionLevel); err != nil {
		return err
	}
	if _, err := getArchiveWriter(config.GCS.CompressionFormat, config.GCS.CompressionLevel); err != nil {
		return err
	}
	return nil
}

// PrintDefaultConfig - print default config to stdout
func PrintDefaultConfig() {
	c := DefaultConfig()
	d, _ := yaml.Marshal(&c)
	fmt.Print(string(d))
}

func DefaultConfig() *Config {
	dc := DestinationConfig{
		CompressionLevel:  1,
		CompressionFormat: "gzip",
	}
	return &Config{
		GeneralConfig: GeneralConfig{
			RemoteStorage:       "s3",
			BackupsToKeepLocal:  0,
			BackupsToKeepRemote: 0,
			BackupNameFormat:    "2006-01-02T15-04-05",
		},
		ClickHouse: ClickHouseConfig{
			Username: "default",
			Password: "",
			Host:     "localhost",
			Port:     9000,
			SkipTables: []string{
				"system.*",
			},
			Timeout: 5 * time.Minute,
		},
		Dir: DirConfig{
			DestinationConfig: dc,
		},
		AzureBlob: AzureBlobConfig{
			DestinationConfig: dc,
			EndpointSuffix:    "core.windows.net",
			TryTimeout:        2 * time.Minute,
			UploadMaxBuffers:  3,
			UploadPartSize:    2 * 1024 * 1024,
		},
		S3: S3Config{
			DestinationConfig: dc,
			Region:            "us-east-1",
			ACL:               "private",
			PartSize:          100 * 1024 * 1024,
		},
		GCS: GCSConfig{
			DestinationConfig: dc,
		},
		COS: COSConfig{
			DestinationConfig: dc,
			Timeout:           2 * time.Minute,
		},
		API: APIConfig{
			ListenAddr: "localhost:7171",
		},
		FTP: FTPConfig{
			DestinationConfig: dc,
			Timeout:           2 * time.Minute,
			TLS:               false,
		},
	}
}
