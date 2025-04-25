package shared

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/csvwriter"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
)

func TempTableID(tableID sql.TableIdentifier) sql.TableIdentifier {
	return TempTableIDWithSuffix(tableID, strings.ToLower(stringutil.Random(5)))
}

func TempTableIDWithSuffix(tableID sql.TableIdentifier, suffix string) sql.TableIdentifier {
	tempTable := fmt.Sprintf(
		"%s_%s_%s_%d",
		tableID.Table(),
		constants.ArtiePrefix,
		suffix,
		time.Now().Add(constants.TemporaryTableTTL).Unix(),
	)
	return tableID.WithTable(tempTable)
}

type File struct {
	FilePath string
	FileName string
}

type ValueConverterFunc func(colValue any, colKind typing.KindDetails, sharedDestinationSettings config.SharedDestinationSettings) (string, error)

func WriteTemporaryTableFile(tableData *optimization.TableData, newTableID sql.TableIdentifier, sharedDestinationSettings config.SharedDestinationSettings, valueConverter ValueConverterFunc) (File, error) {
	fp := filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv.gz", strings.ReplaceAll(newTableID.FullyQualifiedName(), `"`, "")))
	gzipWriter, err := csvwriter.NewGzipWriter(fp)
	if err != nil {
		return File{}, fmt.Errorf("failed to create gzip writer: %w", err)
	}

	defer gzipWriter.Close()

	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	for _, value := range tableData.Rows() {
		var row []string
		for _, col := range columns {
			castedValue, castErr := valueConverter(value[col.Name()], col.KindDetails, sharedDestinationSettings)
			if castErr != nil {
				return File{}, castErr
			}

			row = append(row, castedValue)
		}

		if err = gzipWriter.Write(row); err != nil {
			return File{}, fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	if err = gzipWriter.Flush(); err != nil {
		return File{}, fmt.Errorf("failed to flush gzip writer: %w", err)
	}

	return File{FilePath: fp, FileName: gzipWriter.FileName()}, nil
}
