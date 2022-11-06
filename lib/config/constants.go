package config

import "time"

const (
	ArtiePrefix               = "__artie"
	DeleteColumnMarker        = ArtiePrefix + "_delete"
	DeletionConfidencePadding = 4 * time.Hour
)
