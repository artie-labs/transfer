package databricks

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/artie-labs/transfer/clients/databricks/dialect"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/maputil"
)

type File struct {
	name string
	fp   string
}

func NewFile(fileRow map[string]any) (File, error) {
	name, err := maputil.GetTypeFromMap[string](fileRow, "name")
	if err != nil {
		return File{}, err
	}

	fp, err := maputil.GetTypeFromMap[string](fileRow, "path")
	if err != nil {
		return File{}, err
	}

	return File{name: name, fp: fp}, nil
}

func NewFileFromTableID(tableID dialect.TableIdentifier, volume string) File {
	name := fmt.Sprintf("%s.csv.gz", tableID.Table())
	return File{
		name: name,
		fp:   fmt.Sprintf("/Volumes/%s/%s/%s/%s", tableID.Database(), tableID.Schema(), volume, name),
	}
}

func (f File) Name() string {
	return f.name
}

func (f File) ShouldDelete() bool {
	return ddl.ShouldDeleteFromName(strings.TrimSuffix(f.name, ".csv"))
}

func (f File) DBFSFilePath() string {
	return filepath.Join("dbfs:", f.fp)
}

func (f File) FilePath() string {
	return f.fp
}
