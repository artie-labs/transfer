package main

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"
	"os"
)

func main() {
	ctx := context.Background()
	config.ParseArgs(os.Args, true)
	snowflake.LoadSnowflake(ctx, nil)

	config, err := snowflake.GetTableConfig(ctx, "customers.public.customers_foo")
	fmt.Println("config", config, "err", err)
}
