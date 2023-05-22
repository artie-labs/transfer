package snowflake

import (
	"fmt"
	"strings"
)

// AddPrefixToTableName will take the fully qualified table name and add a prefix in front of the table
// This is necessary for `PUT` commands. The fq name looks like <namespace>.<tableName>
// Namespace may contain both database and schema.
func AddPrefixToTableName(fqTableName string, prefix string) string {
	tableParts := strings.Split(fqTableName, ".")
	if len(tableParts) == 1 {
		return prefix + fqTableName
	}

	return fmt.Sprintf("%s.%s%s",
		strings.Join(tableParts[0:len(tableParts)-1], "."), prefix, tableParts[len(tableParts)-1])
}
