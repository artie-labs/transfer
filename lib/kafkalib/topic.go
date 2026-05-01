package kafkalib

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/artie-labs/transfer/lib/kafkalib/partition"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type DatabaseAndSchemaPair struct {
	Database string
	Schema   string
}

func (d DatabaseAndSchemaPair) IsValid() bool {
	return d.Database != "" && d.Schema != ""
}

func GetUniqueStagingDatabaseAndSchemaPairs(tcs []*TopicConfig) []DatabaseAndSchemaPair {
	seenMap := make(map[DatabaseAndSchemaPair]bool)
	for _, tc := range tcs {
		seenMap[tc.BuildStagingDatabaseAndSchemaPair()] = true
	}

	return slices.Collect(maps.Keys(seenMap))
}

// GetUniqueStagingSchemas returns a deduplicated list of staging schemas from the topic configs.
// This uses GetStagingSchema() which falls back to Schema if StagingSchema is not set.
func GetUniqueStagingSchemas(tcs []*TopicConfig) []string {
	seenMap := make(map[string]bool)
	for _, tc := range tcs {
		seenMap[tc.GetStagingSchema()] = true
	}

	return slices.Collect(maps.Keys(seenMap))
}

// GetAllUniqueSchemas returns a deduplicated list of all schemas (both destination and staging) from the topic configs.
func GetAllUniqueSchemas(tcs []*TopicConfig) []string {
	seenMap := make(map[string]bool)
	for _, tc := range tcs {
		seenMap[tc.Schema] = true
		seenMap[tc.GetStagingSchema()] = true
	}

	return slices.Collect(maps.Keys(seenMap))
}

// ValidateReferenceIDs returns an error if any non-empty referenceID appears more than once across topic configs.
func ValidateReferenceIDs(tcs []*TopicConfig) error {
	firstTopicByRef := make(map[string]string)
	for _, tc := range tcs {
		if tc == nil {
			continue
		}
		ref := tc.ReferenceID
		if ref == "" {
			continue
		}
		if firstTopic, dup := firstTopicByRef[ref]; dup {
			return fmt.Errorf("duplicate referenceID %q (topics %q and %q)", ref, firstTopic, tc.Topic)
		}
		firstTopicByRef[ref] = tc.Topic
	}
	return nil
}

type MultiStepMergeSettings struct {
	Enabled bool `yaml:"enabled"`
	// FlushCount is the number of times we will flush to the multi-step merge table before merging into the destination table.
	FlushCount int `yaml:"flushCount"`
}

func (m MultiStepMergeSettings) Validate() error {
	if !m.Enabled {
		return nil
	}

	if m.FlushCount <= 0 {
		return fmt.Errorf("flush count must be greater than 0")
	}

	return nil
}

type StaticColumn struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type PartitionFrequency string

const (
	Monthly              PartitionFrequency = "monthly"
	Daily                PartitionFrequency = "daily"
	Hourly               PartitionFrequency = "hourly"
	CompactedTableSuffix string             = "_default"
)

func (pf PartitionFrequency) Layout() string {
	switch pf {
	case Monthly:
		return "_2006_01"
	case Daily:
		return "_2006_01_02"
	case Hourly:
		return "_2006_01_02_15"
	}
	return ""
}

func (pf PartitionFrequency) Suffix(value time.Time) (string, error) {
	layout := pf.Layout()
	if layout == "" {
		return "", fmt.Errorf("invalid partition frequency: %q", pf)
	}

	return value.Format(layout), nil
}

// Positive distance means the from time is in a past partition of now.
// Negative distance means the from time is in a future partition of now.
// 0 means the from time is in the same partitionas now.
func (pf PartitionFrequency) PartitionDistance(from, now time.Time) int {
	switch pf {
	case Monthly:
		fromYear, fromMonth, _ := from.Date()
		nowYear, nowMonth, _ := now.Date()
		return (nowYear-fromYear)*12 + int(nowMonth-fromMonth)
	case Daily:
		return int(now.Sub(from).Hours() / 24)
	case Hourly:
		return int(now.Sub(from).Hours())
	}
	return 0
}

type SoftPartitioning struct {
	Enabled            bool               `yaml:"enabled" json:"enabled"`
	PartitionFrequency PartitionFrequency `yaml:"partitionFrequency" json:"partitionFrequency"`
	PartitionColumn    string             `yaml:"partitionColumn" json:"partitionColumn"`
	PartitionSchema    string             `yaml:"partitionSchema" json:"partitionSchema"`
	MaxPartitions      int                `yaml:"maxPartitions" json:"maxPartitions"`
}

func (sp SoftPartitioning) Validate() error {
	if !sp.Enabled {
		return nil
	}
	if sp.PartitionFrequency == "" {
		return fmt.Errorf("partition frequency is required")
	}
	if _, err := sp.PartitionFrequency.Suffix(time.Now()); err != nil {
		return fmt.Errorf("invalid partition frequency: %w", err)
	}
	if sp.PartitionColumn == "" {
		return fmt.Errorf("partition column is required")
	}
	if sp.MaxPartitions <= 0 {
		return fmt.Errorf("maxPartitions must be greater than 0")
	}
	return nil
}

type TopicConfig struct {
	// [ReferenceID] - This is a unique identifier for the topic config. This is used for services that are built on top of Transfer to reference this specific topic config.
	ReferenceID string `yaml:"referenceID,omitempty"`
	Database    string `yaml:"db"`
	Schema      string `yaml:"schema"`
	// [StagingSchema] - Optional schema to use for staging tables. If not specified, Schema will be used.
	StagingSchema string `yaml:"stagingSchema,omitempty"`
	// [TableName] - if left empty, the table name will be deduced from each event.
	TableName                  string `yaml:"tableName"`
	Topic                      string `yaml:"topic"`
	CDCFormat                  string `yaml:"cdcFormat"`
	CDCKeyFormat               string `yaml:"cdcKeyFormat"`
	DropDeletedColumns         bool   `yaml:"dropDeletedColumns"`
	SoftDelete                 bool   `yaml:"softDelete"`
	SkippedOperations          string `yaml:"skippedOperations,omitempty"`
	IncludeArtieUpdatedAt      bool   `yaml:"includeArtieUpdatedAt"`
	IncludeArtieOperation      bool   `yaml:"includeArtieOperation"`
	IncludeDatabaseUpdatedAt   bool   `yaml:"includeDatabaseUpdatedAt"`
	IncludeSourceMetadata      bool   `yaml:"includeSourceMetadata"`
	IncludeFullSourceTableName bool   `yaml:"includeFullSourceTableName"`
	// TODO: Deprecate BigQueryPartitionSettings and use AdditionalMergePredicates instead.
	BigQueryPartitionSettings *partition.BigQuerySettings `yaml:"bigQueryPartitionSettings,omitempty"`
	AdditionalMergePredicates []partition.MergePredicates `yaml:"additionalMergePredicates,omitempty"`
	ColumnsToHash             []string                    `yaml:"columnsToHash,omitempty"`
	// [ColumnsToHashSalt] - Optional customer-provided salt applied to all columns listed in [ColumnsToHash].
	// When set, columns are hashed with HMAC-SHA256 using this salt as the key. When empty, plain SHA-256 is used.
	ColumnsToHashSalt string `yaml:"columnsToHashSalt,omitempty"`

	// [ColumnsToInclude] can be used to specify the exact columns that should be written to the destination.
	ColumnsToInclude []string `yaml:"columnsToInclude,omitempty"`
	// [ColumnsToExclude] can be used to exclude columns from being written to the destination.
	ColumnsToExclude []string `yaml:"columnsToExclude,omitempty"`
	// [ColumnsToEncrypt] can be used to encrypt columns that should be written to the destination.
	// If this is passed in, you must pass in the [SharedDestinationSettings.EncryptionPassphrase] as well.
	ColumnsToEncrypt []string `yaml:"columnsToEncrypt,omitempty"`
	// [EncryptJSONBColumns] - if enabled, we will encrypt the JSONB columns that should be written to the destination.
	// This only works for relational databases where optional schema is available.
	EncryptJSONBColumns bool     `yaml:"encryptJSONBColumns,omitempty"`
	PrimaryKeysOverride []string `yaml:"primaryKeysOverride,omitempty"`
	// [SkipPrimaryKeyCreation] - if enabled, we'll skip creating a primary key on the destination.
	// This is useful when using PrimaryKeysOverride with columns that may contain NULLs.
	SkipPrimaryKeyCreation bool `yaml:"skipPrimaryKeyCreation,omitempty"`

	// [IncludePrimaryKeys] - This is used to specify an additional column that can be used as part of the primary key
	// An example of this could be to include the full source table name.
	IncludePrimaryKeys     []string                `yaml:"includePrimaryKeys,omitempty"`
	MultiStepMergeSettings *MultiStepMergeSettings `yaml:"multiStepMergeSettings,omitempty"`

	// [StaticColumns] can be used to specify static columns that should be written to the destination.
	// This is useful for cases where you want to add additional columns to provide metadata, etc in the destination.
	StaticColumns []StaticColumn `yaml:"staticColumns,omitempty"`

	// [SoftPartitioning] can be used to specify soft partitioning settings for the table.
	SoftPartitioning SoftPartitioning `yaml:"softPartitioning,omitempty"`

	// [AppendOnly] - if true, data will always be appended instead of merged.
	AppendOnly bool `yaml:"appendOnly,omitempty"`

	// [FlushOnReceive] - if true will flush per Kafka batch instead of following flush rules
	FlushOnReceive bool `yaml:"flushOnReceive,omitempty"`
}

func (t TopicConfig) BuildDatabaseAndSchemaPair() DatabaseAndSchemaPair {
	return DatabaseAndSchemaPair{Database: t.Database, Schema: t.Schema}
}

func (t TopicConfig) GetStagingSchema() string {
	return cmp.Or(t.StagingSchema, t.Schema)
}

func (t TopicConfig) BuildStagingDatabaseAndSchemaPair() DatabaseAndSchemaPair {
	return DatabaseAndSchemaPair{Database: t.Database, Schema: t.GetStagingSchema()}
}

// ReusableStagingTableNamePrefix returns the target schema as a prefix when StagingSchema is explicitly
// set to a different value than Schema. This is necessary to prevent name collisions for reusable staging tables
// when multiple topic configs share the same StagingSchema but have different target schemas.
func (t TopicConfig) ReusableStagingTableNamePrefix() string {
	if t.StagingSchema != "" && t.StagingSchema != t.Schema {
		return t.Schema
	}
	return ""
}

const (
	StringKeyFmt = "org.apache.kafka.connect.storage.StringConverter"
	JSONKeyFmt   = "org.apache.kafka.connect.json.JsonConverter"
)

var validKeyFormats = []string{StringKeyFmt, JSONKeyFmt}

func (t TopicConfig) String() string {
	var msmEnabled bool
	if t.MultiStepMergeSettings != nil {
		msmEnabled = t.MultiStepMergeSettings.Enabled
	}

	return fmt.Sprintf("db=%s, schema=%s, tableNameOverride=%s, topic=%s, cdcFormat=%s, dropDeletedColumns=%v, skippedOperations=%v, msmEnabled=%v",
		t.Database, t.Schema, t.TableName, t.Topic, t.CDCFormat, t.DropDeletedColumns, t.SkippedOperations, msmEnabled)
}

func (t TopicConfig) Validate() error {
	if stringutil.Empty(t.Schema, t.Topic, t.CDCFormat) {
		return fmt.Errorf("schema, topic or cdc format is empty")
	}

	if !slices.Contains(validKeyFormats, t.CDCKeyFormat) {
		return fmt.Errorf("invalid cdc key format: %s", t.CDCKeyFormat)
	}

	if t.MultiStepMergeSettings != nil {
		if err := t.MultiStepMergeSettings.Validate(); err != nil {
			return fmt.Errorf("invalid multi-step merge settings: %w", err)
		}
	}

	// You can't specify both [ColumnsToInclude] and [ColumnsToExclude]
	if len(t.ColumnsToInclude) > 0 && len(t.ColumnsToExclude) > 0 {
		return fmt.Errorf("cannot specify both columnsToInclude and columnsToExclude")
	}

	// You cannot have both [PrimaryKeysOverride] and [IncludePrimaryKeys]
	if len(t.PrimaryKeysOverride) > 0 && len(t.IncludePrimaryKeys) > 0 {
		return fmt.Errorf("cannot specify both primaryKeysOverride and includePrimaryKeys")
	}

	if err := t.SoftPartitioning.Validate(); err != nil {
		return fmt.Errorf("invalid soft partitioning configuration: %w", err)
	}

	if len(t.ColumnsToEncrypt) > 0 {
		encryptSet := make(map[string]bool, len(t.ColumnsToEncrypt))
		for _, col := range t.ColumnsToEncrypt {
			encryptSet[col] = true
		}

		for _, pk := range t.PrimaryKeysOverride {
			if encryptSet[pk] {
				return fmt.Errorf("column %q cannot be both a primary key and encrypted, as AES-GCM encryption is non-deterministic", pk)
			}
		}

		for _, pk := range t.IncludePrimaryKeys {
			if encryptSet[pk] {
				return fmt.Errorf("column %q cannot be both a primary key and encrypted, as AES-GCM encryption is non-deterministic", pk)
			}
		}
	}

	return nil
}
