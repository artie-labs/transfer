package transfer

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
)

type Core struct {
	Config        config.Config
	InMemDB       *models.DatabaseData
	Dest          destination.Baseline
	MetricsClient base.Client
}

func NewCore(cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client) Core {
	return Core{
		Config:        cfg,
		InMemDB:       inMemDB,
		Dest:          dest,
		MetricsClient: metricsClient,
	}
}
