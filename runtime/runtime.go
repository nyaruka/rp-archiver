package runtime

import (
	_ "github.com/jackc/pgx/v5/stdlib" // postgres driver
	"github.com/nyaruka/gocommon/aws/cwatch"
	"github.com/nyaruka/gocommon/aws/s3x"
	"github.com/vinovest/sqlx"
)

type Runtime struct {
	Config *Config
	DB     *sqlx.DB
	S3     *s3x.Service
	CW     *cwatch.Service
}
