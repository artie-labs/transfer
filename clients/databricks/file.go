package databricks

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/typing"
)

type File struct {
	name string
	fp   string
}

func NewFile(fileRow map[string]any) (File, error) {
	_volName, isOk := fileRow["name"]
	if !isOk {
		return File{}, fmt.Errorf("name is missing")
	}

	volName, err := typing.AssertType[string](_volName)
	if err != nil {
		return File{}, fmt.Errorf("name is not a string")
	}

	_path, isOk := fileRow["path"]
	if !isOk {
		return File{}, fmt.Errorf("path is missing")
	}

	path, err := typing.AssertType[string](_path)
	if err != nil {
		return File{}, fmt.Errorf("path is not a string")
	}

	return File{name: volName, fp: path}, nil
}

func NewFileFromTableID(tableID TableIdentifier, volume string) File {
	name := fmt.Sprintf("%s.csv", tableID.Table())
	return File{
		name: name,
		fp:   fmt.Sprintf("/Volumes/%s/%s/%s/%s", tableID.Database(), tableID.Schema(), volume, name),
	}

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
