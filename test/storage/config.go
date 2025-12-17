package storage

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
