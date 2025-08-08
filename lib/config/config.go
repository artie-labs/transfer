package config

import (
	"cmp"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/stringutil"
)

const (
	defaultFlushTimeSeconds = 10
	defaultFlushSizeKb      = 25 * 1024 // 25 mb
	defaultBufferPoolSize   = 30000
	bufferPoolSizeMin       = 5

	FlushIntervalSecondsMin = 5
	FlushIntervalSecondsMax = 6 * 60 * 60
)

func (s *S3Settings) Validate() error {
	if s == nil {
		return fmt.Errorf("s3 settings are nil")
	}

	if empty := stringutil.Empty(s.Bucket, s.AwsSecretAccessKey, s.AwsAccessKeyID); empty {
		return fmt.Errorf("one of s3 settings is empty")
	}

	if !constants.IsValidS3OutputFormat(s.OutputFormat) {
		return fmt.Errorf("invalid s3 output format %q", s.OutputFormat)
	}

	return nil
}

func (c Config) TopicConfigs() []*kafkalib.TopicConfig {
	return c.Kafka.TopicConfigs
}

func (m Mode) String() string {
	return string(m)
}

func readFileToConfig(pathToConfig string) (*Config, error) {
	file, err := os.Open(pathToConfig)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	var bytes []byte
	if bytes, err = io.ReadAll(file); err != nil {
		return nil, err
	}

	var config Config
	if err = yaml.Unmarshal(bytes, &config); err != nil {
		return nil, err
	}

	config.Queue = cmp.Or(config.Queue, constants.Kafka)
	config.FlushIntervalSeconds = cmp.Or(config.FlushIntervalSeconds, defaultFlushTimeSeconds)
	config.BufferRows = cmp.Or(config.BufferRows, defaultBufferPoolSize)
	config.FlushSizeKb = cmp.Or(config.FlushSizeKb, defaultFlushSizeKb)
	config.Mode = cmp.Or(config.Mode, Replication)

	return &config, nil
}

func (c Config) ValidateRedshift() error {
	if c.Output != constants.Redshift {
		return fmt.Errorf("output is not Redshift, output: %q", c.Output)
	}

	if c.Redshift == nil {
		return fmt.Errorf("cfg for Redshift is nil")
	}

	if empty := stringutil.Empty(c.Redshift.Host, c.Redshift.Database, c.Redshift.Username,
		c.Redshift.Password, c.Redshift.Bucket, c.Redshift.CredentialsClause); empty {
		return fmt.Errorf("one of Redshift settings is empty")
	}

	if c.Redshift.Port <= 0 {
		return fmt.Errorf("invalid Redshift port")
	}

	return nil
}

func (c Config) ValidateMSSQL() error {
	if c.Output != constants.MSSQL {
		return fmt.Errorf("output is not mssql, output: %v", c.Output)
	}

	if c.MSSQL == nil {
		return fmt.Errorf("mssql config is nil")
	}

	if empty := stringutil.Empty(c.MSSQL.Host, c.MSSQL.Username, c.MSSQL.Password, c.MSSQL.Database); empty {
		return fmt.Errorf("one of mssql settings is empty (host, username, password, database)")
	}

	if c.MSSQL.Port <= 0 {
		return fmt.Errorf("invalid mssql port: %d", c.MSSQL.Port)
	}

	return nil
}

// Validate will check the output source validity
// It will also check if a topic exists + iterate over each topic to make sure it's valid.
// The actual output source (like Snowflake) and CDC parser will be loaded and checked by other funcs.
func (c Config) Validate() error {
	if c.FlushSizeKb <= 0 {
		return fmt.Errorf("flush size pool has to be a positive number, current value: %d", c.FlushSizeKb)
	}

	if !numbers.BetweenEq(FlushIntervalSecondsMin, FlushIntervalSecondsMax, c.FlushIntervalSeconds) {
		return fmt.Errorf("flush interval is outside of our range, seconds: %d, expected start: %d, end: %d",
			c.FlushIntervalSeconds, FlushIntervalSecondsMin, FlushIntervalSecondsMax)
	}

	if bufferPoolSizeMin > int(c.BufferRows) {
		return fmt.Errorf("buffer pool is too small, min value: %d, actual: %d", bufferPoolSizeMin, int(c.BufferRows))
	}

	if !constants.IsValidDestination(c.Output) {
		return fmt.Errorf("invalid destination: %s", c.Output)
	}

	switch c.Output {
	case constants.MSSQL:
		if err := c.ValidateMSSQL(); err != nil {
			return err
		}
	case constants.Redshift:
		if err := c.ValidateRedshift(); err != nil {
			return err
		}
	case constants.S3:
		if err := c.S3.Validate(); err != nil {
			return err
		}
	}

	switch c.Queue {
	case constants.Kafka:
		if c.Kafka == nil {
			return fmt.Errorf("kafka config is nil")
		}

		// Username and password may not be required if this is connecting to a private Kafka cluster.
		if stringutil.Empty(c.Kafka.GroupID, c.Kafka.BootstrapServer) {
			return fmt.Errorf("kafka group or bootstrap server is empty")
		}
	}

	tcs := c.TopicConfigs()
	if len(tcs) == 0 {
		return fmt.Errorf("no topic configs found")
	}

	for _, topicConfig := range tcs {
		if err := topicConfig.Validate(); err != nil {
			return fmt.Errorf("failed to validate topic config: %w", err)
		}

		// History Mode Validation
		if c.Mode == History {
			if topicConfig.DropDeletedColumns {
				return fmt.Errorf("dropDeletedColumns is not supported in history mode, topic: %s", topicConfig.String())
			}

			if !topicConfig.IncludeDatabaseUpdatedAt {
				return fmt.Errorf("includeDatabaseUpdatedAt is required in history mode, topic: %s", topicConfig.String())
			}
		}

	}

	return nil
}
