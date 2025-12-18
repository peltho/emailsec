package storage

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/suite"

	"stoik.com/emailsec/internal/storage"
	"stoik.com/emailsec/test"
)

func TestIngestion(t *testing.T) {
	suite.Run(t, new(IngestionSuite))
}

type IngestionSuite struct {
	suite.Suite
	dockerPool       *dockertest.Pool
	postgresResource *dockertest.Resource
	postgresDB       *sql.DB
	storage          *storage.EmailsStorage
}

func (suite *IngestionSuite) SetupSuite() {
	pool, err := dockertest.NewPool("")
	if err != nil {
		suite.T().Fatalf("Could not connect to docker: %s", err)
	}
	suite.dockerPool = pool
	db, port, postgresResource := test.SetupPostgresDB(suite.T(), pool)
	suite.postgresDB = db
	suite.postgresResource = postgresResource

	if !suite.T().Failed() {
		ctx := context.Background()
		postgresDB, err := storage.NewPostgresDB(ctx, test.PostgresHost, port, test.PostgresUser, test.PostgresPassword, test.PostgresDB)
		if err != nil {
			suite.T().Fatalf("Failed to connect to database: %v", err)
		}

		suite.storage = storage.NewEmailsStorage(postgresDB)
	}
}

func (suite *IngestionSuite) SetupTest() {
	test.ExecFile(suite.T(), suite.postgresDB, "../sql/create_tables.sql")
	test.ExecFile(suite.T(), suite.postgresDB, "../sql/fixtures.sql")

	if suite.T().Failed() {
		suite.TearDownSuite()
		suite.T().FailNow()
	}
}

func (suite *IngestionSuite) TearDownSuite() {
	if suite.postgresDB != nil {
		_ = suite.postgresDB.Close()
	}
	if suite.dockerPool != nil {
		if suite.postgresResource != nil {
			_ = suite.dockerPool.Purge(suite.postgresResource)
		}
	}
}

func (suite *IngestionSuite) TestGetTenant_OK() {
	ctx := context.Background()
	tenantID, _ := uuid.Parse("d290f1ee-6c54-4b01-90e6-d701748f0851")
	tenant, err := suite.storage.GetTenant(ctx, tenantID)

	suite.NoError(err)
	suite.Assert().Equal("Voyage Privé", tenant.Name)
}

// Other storage methods can be tested here
