package database_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xinoip/go-database"
)

func TestNewConnWithPostgis_OK(t *testing.T) {
	ctx := context.Background()

	RunWithDB(t, func(connURL string) {
		params := database.ConnParams{
			Addr:       connURL,
			HasPostgis: true,
		}

		conn, err := database.NewConn(ctx, &params)
		assert.NoError(t, err)
		assert.NotNil(t, conn)

		conn.Close()
		err = conn.Ping(ctx)
		assert.Error(t, err)
	})
}

func TestNewConnWithoutPostgis_OK(t *testing.T) {
	ctx := context.Background()

	RunWithDB(t, func(connURL string) {
		params := database.ConnParams{
			Addr:       connURL,
			HasPostgis: false,
		}

		conn, err := database.NewConn(ctx, &params)
		assert.NoError(t, err)
		assert.NotNil(t, conn)

		err = conn.Ping(ctx)
		assert.NoError(t, err)

		conn.Close()
		err = conn.Ping(ctx)
		assert.Error(t, err)
	})
}

func TestNewConn_InvalidParams(t *testing.T) {
	ctx := context.Background()

	invalidConnURL := "invalid-connection-string"
	params := database.ConnParams{
		Addr:       invalidConnURL,
		HasPostgis: false,
	}

	conn, err := database.NewConn(ctx, &params)
	assert.Error(t, err)
	assert.Nil(t, conn)
}
