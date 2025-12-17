package test

import (
	"database/sql"
	"os"
	"testing"
)

// Postgres test database configuration
const (
	PostgresUser     = "emailsec"
	PostgresPassword = "emailsec_pwd"
	PostgresDB       = "emailsec_test"
	PostgresHost     = "localhost"
)

// PostgresDSN returns the data source name for Postgres connection with dynamic port
func PostgresDSN(port string) string {
	return "postgres://" + PostgresUser + ":" + PostgresPassword + "@" + PostgresHost + ":" + port + "/" + PostgresDB + "?sslmode=disable"
}

// PostgresDockerEnv returns the environment variables for Postgres Docker container
func PostgresDockerEnv() []string {
	return []string{
		"POSTGRES_USER=" + PostgresUser,
		"POSTGRES_PASSWORD=" + PostgresPassword,
		"POSTGRES_DB=" + PostgresDB,
	}
}

func ExecFile(t *testing.T, db *sql.DB, file string) {
	if t.Failed() {
		return
	}
	fileContent, err := os.ReadFile(file)
	if err != nil {
		t.Errorf("cannot read sql file %v", err)
		return
	}
	sql := string(fileContent)
	_, err = db.Exec(sql)
	if err != nil {
		t.Errorf("cannot execute sql file %v", err)
		return
	}
}
