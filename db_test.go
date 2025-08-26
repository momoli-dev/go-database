package database_test

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/xinoip/go-database"
)

// testDB is the global shared test database instance.
var testDB *TestDB

// TestDB is a test database container.
type TestDB struct {
	pg      *postgres.PostgresContainer
	connURL string
}

// NewTestDB creates TestDB.
func NewTestDB(t *testing.T) *TestDB {
	ctx := context.Background()

	dbName := "go_database_test"
	dbUser := "admin"
	dbPass := "password"

	pg, err := postgres.Run(ctx,
		"postgis/postgis:17-3.5",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPass),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err, "Could not start postgres container")

	connURL, err := pg.ConnectionString(ctx)
	require.NoError(t, err, "Could not get connection string")

	return &TestDB{
		pg:      pg,
		connURL: connURL,
	}
}

// Close terminates the container, potentially exiting on error.
func (tdb *TestDB) Close() {
	if err := tdb.pg.Terminate(context.Background()); err != nil {
		log.Panicf("Could not terminate container: %s", err)
	}
}

// ConnURL returns the connection URL for the test database.
func (tdb *TestDB) ConnURL() string {
	return tdb.connURL
}

// ResetDB resets, closes and re-opens the global test database instance.
func ResetDB(t *testing.T) {
	if testDB != nil {
		testDB.Close()
		testDB = NewTestDB(t)
	}
}

// RunWithDB helper opens a test database, runs f, and closes the database at the end.
func RunWithDB(t *testing.T, f func(connURL string)) {
	if testDB == nil {
		testDB = NewTestDB(t)
	}
	f(testDB.ConnURL())
}

// RunWithConn helper opens a test database connection, runs f, and closes the connection at the end.
func RunWithConn(t *testing.T, f func(conn *database.Conn)) {
	RunWithDB(t, func(connURL string) {
		conn, err := database.NewConn(context.Background(), &database.ConnParams{
			Addr:       connURL,
			HasPostgis: true,
		})
		require.NoError(t, err)
		require.NotNil(t, conn)
		f(conn)
	})
}
