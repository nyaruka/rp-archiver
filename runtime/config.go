package runtime

// Config is our top level configuration object
type Config struct {
	DB        string `help:"the connection string for our database"`
	LogLevel  string `help:"the log level, one of error, warn, info, debug"`
	SentryDSN string `help:"the sentry configuration to log errors to, if any"`

	AWSAccessKeyID     string `help:"access key ID to use for AWS services"`
	AWSSecretAccessKey string `help:"secret access key to use for AWS services"`
	AWSRegion          string `help:"region to use for AWS services, e.g. us-east-1"`

	S3Endpoint string `help:"S3 endpoint we will write archives to"`
	S3Bucket   string `help:"S3 bucket we will write archives to"`
	S3Minio    bool   `help:"S3 is actually Minio or other compatible service"`

	TempDir       string `help:"directory where temporary archive files are written"`
	CheckS3Hashes bool   `help:"whether to check S3 hashes of uploaded archives before deleting records"`

	ArchiveMessages bool   `help:"whether we should archive messages"`
	ArchiveRuns     bool   `help:"whether we should archive runs"`
	RetentionPeriod int    `help:"the number of days to keep before archiving"`
	Delete          bool   `help:"whether to delete messages and runs from the db after archival (default false)"`
	StartTime       string `help:"what time archive jobs should run in UTC HH:MM "`
	Once            bool   `help:"whether archiver should run once and exit (default false)"`

	CloudwatchNamespace string `help:"the namespace to use for cloudwatch metrics"`
	DeploymentID        string `help:"the deployment identifier to use for metrics"`
}

// NewDefaultConfig returns a new default configuration object
func NewDefaultConfig() *Config {

	return &Config{
		DB: "postgres://localhost/archiver_test?sslmode=disable",

		AWSAccessKeyID:     "",
		AWSSecretAccessKey: "",
		AWSRegion:          "us-east-1",

		S3Endpoint: "https://s3.amazonaws.com",
		S3Bucket:   "temba-archives",
		S3Minio:    false,

		TempDir:       "/tmp",
		CheckS3Hashes: true,

		ArchiveMessages: true,
		ArchiveRuns:     true,
		RetentionPeriod: 90,
		Delete:          false,
		StartTime:       "00:01",
		Once:            false,

		CloudwatchNamespace: "Temba/Archiver",
		DeploymentID:        "dev",

		LogLevel: "info",
	}
}
