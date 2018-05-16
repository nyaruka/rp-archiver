package archiver

type Config struct {
	DB        string `help:"the connection string for our database"`
	LogLevel  string `help:"the log level, one of error, warn, info, debug"`
	SentryDSN string `help:"the sentry configuration to log errors to, if any"`

	S3Endpoint       string `help:"the S3 endpoint we will write archives to"`
	S3Region         string `help:"the S3 region we will write archives to"`
	S3Bucket         string `help:"the S3 bucket we will write archives to"`
	S3DisableSSL     bool   `help:"whether we disable SSL when accessing S3. Should always be set to False unless you're hosting an S3 compatible service within a secure internal network"`
	S3ForcePathStyle bool   `help:"whether we force S3 path style. Should generally need to default to False unless you're hosting an S3 compatible service"`

	AWSAccessKeyID     string `help:"the access key id to use when authenticating S3"`
	AWSSecretAccessKey string `help:"the secret access key id to use when authenticating S3"`

	TempDir           string `help:"directory where temporary archive files are written"`
	DeleteAfterUpload bool   `help:"whether we should delete temporary archive file"`
	UploadToS3        bool   `help:"whether we should upload archive to S3"`

	ArchiveMessages bool `help:"whether we should archive messages"`
	ArchiveRuns     bool `help:"whether we should archive runs"`
}

func NewConfig() *Config {
	config := Config{
		DB:       "postgres://localhost/temba?sslmode=disable",
		LogLevel: "info",

		S3Endpoint:       "https://s3.amazonaws.com",
		S3Region:         "us-east-1",
		S3Bucket:         "dl-temba-archives",
		S3DisableSSL:     false,
		S3ForcePathStyle: false,

		AWSAccessKeyID:     "missing_aws_access_key_id",
		AWSSecretAccessKey: "missing_aws_secret_access_key",

		TempDir:           "/tmp/archiver",
		DeleteAfterUpload: true,
		UploadToS3:        true,

		ArchiveMessages: true,
		ArchiveRuns:     true,
	}

	return &config
}
