package partition

type PartitionType string

const (
	TimePartitionType    PartitionType = "time"
	IntegerPartitionType PartitionType = "integer"
)

var ValidPartitionTypes = []PartitionType{
	TimePartitionType,
	IntegerPartitionType,
}

// TODO: We should be able to support different partition by fields in the future.
// https://cloud.google.com/bigquery/docs/partitioned-tables#partition_decorators
var ValidPartitionBy = []string{
	"daily",
}

// We need the JSON annotations here for our dashboard to import the settings correctly.

type MergePredicates struct {
	PartitionField string `yaml:"partitionField" json:"partitionField"`

	// TODO - Start using this.
	PartitionBy   string        `yaml:"partitionBy" json:"partitionBy"`
	PartitionType PartitionType `yaml:"partitionType" json:"partitionType"`
}

type BigQuerySettings struct {
	PartitionType  PartitionType `yaml:"partitionType" json:"partitionType"`
	PartitionField string        `yaml:"partitionField" json:"partitionField"`
	PartitionBy    string        `yaml:"partitionBy" json:"partitionBy"`
}
