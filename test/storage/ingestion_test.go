package storage

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/suite"

	"stoik.com/emailsec/internal/storage"
)

func TestIngestion(t *testing.T) {
	suite.Run(t, new(IngestionSuite))
}

type IngestionSuite struct {
	suite.Suite
	dockerPool           *dockertest.Pool
	postgresCoreResource *dockertest.Resource
	postgresDB           *sql.DB
	storage              *storage.EmailsStorage
}

func (suite *IngestionSuite) SetupSuite() {
	pool, err := dockertest.NewPool("")
	if err != nil {
		suite.T().Fatalf("Could not connect to docker: %s", err)
	}
	suite.dockerPool = pool

	suite.postgresCoreResource, err = pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16-alpine",
		Env:        PostgresDockerEnv(),
	})
	if err != nil {
		suite.T().Fatalf("Could not run postgres from docker: %s", err)
	}

	// Get the dynamically assigned port
	port := suite.postgresCoreResource.GetPort("5432/tcp")

	// Retry connection until Docker container is ready
	if err = pool.Retry(func() error {
		var err error
		suite.postgresDB, err = sql.Open("pgx", PostgresDSN(port))
		if err != nil {
			return err
		}
		return suite.postgresDB.Ping()
	}); err != nil {
		suite.T().Fatalf("Could not connect to postgres: %s", err)
	}

	if !suite.T().Failed() {
		ctx := context.Background()
		postgresDB, err := storage.NewPostgresDB(ctx, PostgresHost, port, PostgresUser, PostgresPassword, PostgresDB)
		if err != nil {
			suite.T().Fatalf("Failed to connect to database: %v", err)
		}

		suite.storage = storage.NewEmailsStorage(postgresDB)
	}
}

func (suite *IngestionSuite) ExecFile(t *testing.T, file string) {
	if t.Failed() {
		return
	}
	fileContent, err := os.ReadFile(file)
	if err != nil {
		t.Errorf("cannot read sql file %v", err)
		return
	}
	sql := string(fileContent)
	_, err = suite.postgresDB.Exec(sql)
	if err != nil {
		t.Errorf("cannot execute sql file %v", err)
		return
	}
}

func (suite *IngestionSuite) SetupTest() {
	suite.ExecFile(suite.T(), "../sql/create_tables.sql")
	suite.ExecFile(suite.T(), "../sql/fixtures.sql")
	if suite.T().Failed() {
		suite.TearDownSuite()
		suite.T().FailNow()
	}
}

func (suite *IngestionSuite) TearDownSuite() {
	if suite.postgresDB != nil {
		_ = suite.postgresDB.Close()
	}
	if suite.postgresCoreResource != nil {
		_ = suite.dockerPool.Purge(suite.postgresCoreResource)
	}
}

func (suite *IngestionSuite) TestGetTenant_OK() {
	ctx := context.Background()
	tenantID, _ := uuid.Parse("d290f1ee-6c54-4b01-90e6-d701748f0851")
	tenant, err := suite.storage.GetTenant(ctx, tenantID)

	suite.NoError(err)
	suite.Assert().Equal("Voyage Privé", tenant.Name)
}
