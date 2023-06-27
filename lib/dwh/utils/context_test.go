package utils

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/db/mock"
	"github.com/artie-labs/transfer/lib/dwh"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/assert"
)

func TestInjectDwhIntoCtx(t *testing.T) {
	ctx := context.Background()

	store := db.Store(&mock.DB{
		Fake: mocks.FakeStore{},
	})

	// Check before injection, there should be no DWH.
	dwhVal := ctx.Value(dwhKey)
	assert.Nil(t, dwhVal)

	var _dwh dwh.DataWarehouse
	_dwh = snowflake.LoadSnowflake(ctx, &store)

	ctx = InjectDwhIntoCtx(_dwh, ctx)
	dwhCtx := FromContext(ctx)
	assert.NotNil(t, dwhCtx)
	assert.Equal(t, dwhCtx, _dwh)
}
