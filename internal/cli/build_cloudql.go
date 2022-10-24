//go:build cloudsql
// +build cloudsql

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/cloudsql"
)
