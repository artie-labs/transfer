package shared

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func IsReusableStagingTable(tableName string, suffix string) bool {
	return constants.ArtiePrefix != "" &&
		len(tableName) > len(constants.ArtiePrefix)+len(suffix) &&
		tableName[len(tableName)-len(suffix):] == suffix &&
		tableName[len(tableName)-len(suffix)-len(constants.ArtiePrefix)-1:len(tableName)-len(suffix)] == "_"+constants.ArtiePrefix
}

func GenerateReusableStagingTableName(baseTableName string, suffix string) string {
	return fmt.Sprintf("%s_%s%s", baseTableName, constants.ArtiePrefix, suffix)
}
