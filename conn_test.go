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

		pool := conn.Pool()
		assert.NotNil(t, pool)

		handle := conn.Handle()
		assert.NotNil(t, handle)

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

func TestNewConn_Unreachable(t *testing.T) {
	ctx := context.Background()

	unreachableConnURL := "postgres://invalid:invalid@localhost:5432/invalid"
	params := database.ConnParams{
		Addr:       unreachableConnURL,
		HasPostgis: false,
	}

	conn, err := database.NewConn(ctx, &params)
	assert.Error(t, err)
	assert.Nil(t, conn)
}
