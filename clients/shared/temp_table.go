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

type AdditionalOutput struct {
	ColumnToNewLengthMap map[string]int32
}

type ValueConvertResponse struct {
	Value string
	// NewLength - If the value exceeded the maximum length, this will be the new length of the value.
	// This is only applicable if [expandStringPrecision] is enabled.
	NewLength int32
	Exceeded  bool
}

type ValueConverterFunc func(colValue any, colKind typing.KindDetails, sharedDestinationSettings config.SharedDestinationSettings) (ValueConvertResponse, error)

type TemporaryDataFile struct {
	fileName string
}

func NewTemporaryDataFile(newTableID sql.TableIdentifier) TemporaryDataFile {
	return TemporaryDataFile{
		fileName: fmt.Sprintf("%s_%s.csv.gz", strings.ReplaceAll(newTableID.FullyQualifiedName(), `"`, ""), stringutil.Random(10)),
	}
}

func NewTemporaryDataFileWithFileName(fileName string) TemporaryDataFile {
	return TemporaryDataFile{
		fileName: fileName,
	}
}

func (t TemporaryDataFile) WriteTemporaryTableFile(tableData *optimization.TableData, valueConverter ValueConverterFunc, sharedDestinationSettings config.SharedDestinationSettings) (File, AdditionalOutput, error) {
	fp := filepath.Join(os.TempDir(), t.fileName)
	gzipWriter, err := csvwriter.NewGzipWriter(fp)
	if err != nil {
		return File{}, AdditionalOutput{}, fmt.Errorf("failed to create gzip writer: %w", err)
	}

	defer gzipWriter.Close()

	columnToNewLengthMap := make(map[string]int32)
	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	for _, value := range tableData.Rows() {
		var row []string
		for _, col := range columns {
			result, castErr := valueConverter(value[col.Name()], col.KindDetails, sharedDestinationSettings)
			if castErr != nil {
				return File{}, AdditionalOutput{}, castErr
			}

			fmt.Println("colName", col.Name(), "value", result.Value, fmt.Sprintf("type: %T", value[col.Name()]))

			if result.NewLength > 0 {
				_newLength, ok := columnToNewLengthMap[col.Name()]
				if result.NewLength > _newLength || !ok {
					columnToNewLengthMap[col.Name()] = result.NewLength
				}
			}

			row = append(row, result.Value)
		}

		if err = gzipWriter.Write(row); err != nil {
			return File{}, AdditionalOutput{}, fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	if err = gzipWriter.Flush(); err != nil {
		return File{}, AdditionalOutput{}, fmt.Errorf("failed to flush gzip writer: %w", err)
	}

	return File{FilePath: fp, FileName: gzipWriter.FileName()}, AdditionalOutput{ColumnToNewLengthMap: columnToNewLengthMap}, nil
}
