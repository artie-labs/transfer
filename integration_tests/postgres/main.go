package postgres

import (
	"cmp"
	"log"
	"os"

	"github.com/artie-labs/transfer/lib/config"
	"gorm.io/driver/postgres"
)

func main() {
	cfg := config.Postgres{
		Host:     cmp.Or(os.Getenv("PG_HOST"), "localhost"),
		Port:     5432,
		Username: "postgres",
		Password: "postgres",
		Database: "postgres",
	}

	store, err := postgres.New(cfg)
	if err != nil {
		log.Fatalf("failed to create postgres client: %v", err)
	}

	db, err := store.DB()
	if err != nil {
		log.Fatalf("failed to get postgres client: %v", err)
	}

}
