package database_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xinoip/go-database"
)

func TestNoTx(t *testing.T) {
	ctx := context.Background()

	inTx := database.InTx(ctx)
	assert.False(t, inTx)

	tx := database.Tx(ctx)
	assert.Nil(t, tx)
}

func TestBeginTx_OK(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		newCtx, err := conn.BeginTx(ctx)
		assert.NoError(t, err)

		inTx := database.InTx(newCtx)
		assert.True(t, inTx)

		tx := database.Tx(newCtx)
		assert.NotNil(t, tx)
	})
}

func TestBeginTx_AlreadyInTx(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		newCtx, err := conn.BeginTx(ctx)
		assert.NoError(t, err)

		sameCtx, err := conn.BeginTx(newCtx)
		assert.NoError(t, err)
		assert.Equal(t, newCtx, sameCtx)

		inTx := database.InTx(sameCtx)
		assert.True(t, inTx)
	})
}

func TestCommitTx_OK(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		newCtx, err := conn.BeginTx(ctx)
		assert.NoError(t, err)

		err = database.CommitTx(newCtx)
		assert.NoError(t, err)
	})
}

func TestCommitTx_NoTx(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		err := database.CommitTx(ctx)
		assert.NoError(t, err)
	})
}

func TestRollbackTx_OK(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		newCtx, err := conn.BeginTx(ctx)
		assert.NoError(t, err)

		err = database.RollbackTx(newCtx)
		assert.NoError(t, err)
	})
}

func TestRollbackTx_NoTx(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		err := database.RollbackTx(ctx)
		assert.NoError(t, err)
	})
}

func TestWithTx_OK(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		err := conn.WithTx(ctx, func(txCtx context.Context) error {
			inTx := database.InTx(txCtx)
			assert.True(t, inTx)

			tx := database.Tx(txCtx)
			assert.NotNil(t, tx)

			return nil
		})
		assert.NoError(t, err)
	})
}

func TestWithTx_NestedTx(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		err := conn.WithTx(ctx, func(txCtx context.Context) error {
			inTx := database.InTx(txCtx)
			assert.True(t, inTx)

			tx := database.Tx(txCtx)
			assert.NotNil(t, tx)

			return conn.WithTx(txCtx, func(nestedTxCtx context.Context) error {
				nestedInTx := database.InTx(nestedTxCtx)
				assert.True(t, nestedInTx)

				nestedTx := database.Tx(nestedTxCtx)
				assert.NotNil(t, nestedTx)

				assert.Equal(t, tx, nestedTx)
				assert.Equal(t, txCtx, nestedTxCtx)

				return nil
			})
		})
		assert.NoError(t, err)
	})
}

func TestWithTx_ErrorInFunc(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		err := conn.WithTx(ctx, func(txCtx context.Context) error {
			return assert.AnError
		})
		assert.Error(t, err)
	})
}

func TestWithTxHelper_OK(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		result, err := database.WithTx(ctx, conn, func(txCtx context.Context) (string, error) {
			inTx := database.InTx(txCtx)
			assert.True(t, inTx)

			tx := database.Tx(txCtx)
			assert.NotNil(t, tx)

			return "success", nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "success", result)
	})
}

func TestWithTxHelper_ErrorInFunc(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		result, err := database.WithTx(ctx, conn, func(txCtx context.Context) (string, error) {
			return "", assert.AnError
		})
		assert.Error(t, err)
		assert.Equal(t, "", result)
	})
}

func TestWithTxHelper_Nested(t *testing.T) {
	RunWithConn(t, func(conn *database.Conn) {
		ctx := context.Background()
		res, err := database.WithTx(ctx, conn, func(txCtx context.Context) (string, error) {
			inTx := database.InTx(txCtx)
			assert.True(t, inTx)

			tx := database.Tx(txCtx)
			assert.NotNil(t, tx)

			return database.WithTx(txCtx, conn, func(nestedTxCtx context.Context) (string, error) {
				nestedInTx := database.InTx(nestedTxCtx)
				assert.True(t, nestedInTx)

				nestedTx := database.Tx(nestedTxCtx)
				assert.NotNil(t, nestedTx)

				assert.Equal(t, tx, nestedTx)
				assert.Equal(t, txCtx, nestedTxCtx)

				return "nested success", nil
			})
		})
		assert.Equal(t, "nested success", res)
		assert.NoError(t, err)
	})
}
