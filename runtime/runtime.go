package runtime

import (
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/aws/s3x"
)

type Runtime struct {
	Config *Config
	DB     *sqlx.DB
	S3     *s3x.Service
}
