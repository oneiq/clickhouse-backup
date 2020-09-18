module github.com/AlexAkulov/clickhouse-backup

require (
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.10.1-0.20200807102407-24fe552e0870
	github.com/ClickHouse/clickhouse-go v1.4.3
	github.com/andybalholm/brotli v1.0.0 // indirect
	github.com/aws/aws-sdk-go v1.34.10
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/djherbis/buffer v1.1.0 // indirect
	github.com/fatih/color v1.9.0 // indirect
	github.com/frankban/quicktest v1.10.0 // indirect
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.7.4
	github.com/jlaffaye/ftp v0.0.0-20200730135723-c2ee4fa2503b
	github.com/jmoiron/sqlx v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/klauspost/compress v1.9.4 // indirect
	github.com/mattn/go-colorable v0.1.6 // indirect
	github.com/mattn/go-runewidth v0.0.7 // indirect
	github.com/mholt/archiver v1.1.3-0.20190812163345-2d1449806793
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/stretchr/testify v1.6.1
	github.com/tencentyun/cos-go-sdk-v5 v0.0.0-20200120023323-87ff3bc489ac
	github.com/urfave/cli v1.22.2
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
	google.golang.org/api v0.28.0
	gopkg.in/cheggaaa/pb.v1 v1.0.28
	gopkg.in/djherbis/buffer.v1 v1.1.0
	gopkg.in/djherbis/nio.v2 v2.0.3
	gopkg.in/yaml.v2 v2.3.0
)

replace github.com/kelseyhightower/envconfig => ./pkg/envconfig

go 1.14
