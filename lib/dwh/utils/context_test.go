package utils

import (
	"context"
	dwh2 "github.com/artie-labs/transfer/lib/dwh"
	"testing"

	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/db/mock"
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

	var dwh dwh2.DataWarehouse
	dwh = snowflake.LoadSnowflake(ctx, &store)

	ctx = InjectDwhIntoCtx(dwh, ctx)
	dwhCtx := FromContext(ctx)
	assert.NotNil(t, dwhCtx)
	assert.Equal(t, dwhCtx, dwh)
}
