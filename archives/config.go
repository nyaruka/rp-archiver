package archives

import "os"

// Config is our top level configuration object
type Config struct {
	DB        string `help:"the connection string for our database"`
	LogLevel  string `help:"the log level, one of error, warn, info, debug"`
	SentryDSN string `help:"the sentry configuration to log errors to, if any"`

	AWSAccessKeyID     string `help:"access key ID to use for AWS services"`
	AWSSecretAccessKey string `help:"secret access key to use for AWS services"`
	AWSRegion          string `help:"region to use for AWS services, e.g. us-east-1"`

	S3Endpoint string `help:"the S3 endpoint we will write archives to"`
	S3Bucket   string `help:"the S3 bucket we will write archives to"`

	TempDir       string `help:"directory where temporary archive files are written"`
	KeepFiles     bool   `help:"whether we should keep local archive files after upload (default false)"`
	UploadToS3    bool   `help:"whether we should upload archive to S3"`
	CheckS3Hashes bool   `help:"whether to check S3 hashes of uploaded archives before deleting records"`

	ArchiveMessages bool   `help:"whether we should archive messages"`
	ArchiveRuns     bool   `help:"whether we should archive runs"`
	RetentionPeriod int    `help:"the number of days to keep before archiving"`
	Delete          bool   `help:"whether to delete messages and runs from the db after archival (default false)"`
	StartTime       string `help:"what time archive jobs should run in UTC HH:MM "`
	Once            bool   `help:"whether archiver should run once and exit (default false)"`

	LibratoUsername string `help:"the username that will be used to authenticate to Librato"`
	LibratoToken    string `help:"the token that will be used to authenticate to Librato"`
	InstanceName    string `help:"the unique name of this instance used for analytics"`
}

// NewDefaultConfig returns a new default configuration object
func NewDefaultConfig() *Config {
	hostname, _ := os.Hostname()

	config := Config{
		DB: "postgres://localhost/archiver_test?sslmode=disable",

		AWSAccessKeyID:     "",
		AWSSecretAccessKey: "",
		AWSRegion:          "us-east-1",

		S3Endpoint: "https://s3.amazonaws.com",
		S3Bucket:   "dl-archiver-test",

		TempDir:       "/tmp",
		KeepFiles:     false,
		UploadToS3:    true,
		CheckS3Hashes: true,

		ArchiveMessages: true,
		ArchiveRuns:     true,
		RetentionPeriod: 90,
		Delete:          false,
		StartTime:       "00:01",
		Once:            false,

		InstanceName: hostname,
		LogLevel:     "info",
	}

	return &config
}
