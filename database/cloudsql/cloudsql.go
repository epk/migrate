package cloudsql

import (
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"

	cloudsqlmysql "cloud.google.com/go/cloudsqlconn/mysql/mysql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/mysql"
)

var (
	driverType = "cloudsql-mysql"
)

var _ database.Driver = (*Cloudsql)(nil)

func init() {
	database.Register(driverType, &Cloudsql{})
}

type Cloudsql struct {
	inner database.Driver
}

func (c *Cloudsql) Open(url string) (database.Driver, error) {
	dsn, err := buildDSN(url)
	if err != nil {
		return nil, err
	}

	_, err = cloudsqlmysql.RegisterDriver(driverType)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driverType, dsn)
	if err != nil {
		return nil, err
	}

	wrappedDriver, err := mysql.WithInstance(db, &mysql.Config{})
	if err != nil {
		return nil, err
	}

	c.inner = wrappedDriver
	return c, nil
}

func (c *Cloudsql) Close() error {
	return c.inner.Close()
}

func (c *Cloudsql) Lock() error {
	return c.inner.Lock()
}

func (c *Cloudsql) Unlock() error {
	return c.inner.Unlock()
}

func (c *Cloudsql) Run(migration io.Reader) error {
	return c.inner.Run(migration)
}

func (c *Cloudsql) SetVersion(version int, dirty bool) error {
	return c.inner.SetVersion(version, dirty)
}

func (c *Cloudsql) Version() (version int, dirty bool, err error) {
	return c.inner.Version()
}

func (c *Cloudsql) Drop() error {
	return c.inner.Drop()
}

// Must match the following
// my-user:mypass@cloudsql-mysql(my-proj:us-central1:my-inst)/my-db
// Custom params for mysql driver configuration are currently all stripped away
func buildDSN(url string) (string, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return "", err
	}

	dsn := migrate.FilterCustomQuery(purl).String()
	return fmt.Sprintf("%s?multiStatements=true", dsn), nil
}
