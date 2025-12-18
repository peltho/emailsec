package test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	amqp "github.com/rabbitmq/amqp091-go"
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

func SetupPostgresDB(t *testing.T, pool *dockertest.Pool) (*sql.DB, string, *dockertest.Resource) {
	postgresResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16-alpine",
		Env:        PostgresDockerEnv(),
	})
	if err != nil {
		t.Fatalf("Could not run postgres from docker: %s", err)
	}

	// Get the dynamically assigned port
	port := postgresResource.GetPort("5432/tcp")

	var db *sql.DB
	// Retry connection until Docker container is ready
	if err = pool.Retry(func() error {
		var err error
		db, err = sql.Open("pgx", PostgresDSN(port))
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		t.Fatalf("Could not connect to postgres: %s", err)
	}

	return db, port, postgresResource
}

// RabbitMQ test configuration
const (
	RabbitMQUser     = "guest"
	RabbitMQPassword = "guest"
	RabbitMQHost     = "localhost"
)

// RabbitMQURL returns the AMQP URL for RabbitMQ connection with dynamic port
func RabbitMQURL(port string) string {
	return "amqp://" + RabbitMQUser + ":" + RabbitMQPassword + "@" + RabbitMQHost + ":" + port + "/"
}

// SetupRabbitMQ starts a RabbitMQ Docker container and returns a connection and port
func SetupRabbitMQ(t *testing.T, pool *dockertest.Pool) (*amqp.Connection, *dockertest.Resource) {
	rabbitResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "rabbitmq",
		Tag:        "3-management-alpine",
	})
	if err != nil {
		t.Fatalf("Could not run rabbitmq from docker: %s", err)
	}

	// Get the dynamically assigned port
	port := rabbitResource.GetPort("5672/tcp")

	var conn *amqp.Connection
	// Retry connection until Docker container is ready
	if err = pool.Retry(func() error {
		var err error
		conn, err = amqp.Dial(RabbitMQURL(port))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to rabbitmq: %s", err)
	}

	return conn, rabbitResource
}
