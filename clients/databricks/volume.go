package databricks

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/typing"
)

type Volume struct {
	name string
	path string
}

func NewVolume(volumeRow map[string]any) (Volume, error) {
	_volName, isOk := volumeRow["name"]
	if !isOk {
		return Volume{}, fmt.Errorf("volume name is missing")
	}

	volName, err := typing.AssertType[string](_volName)
	if err != nil {
		return Volume{}, fmt.Errorf("volume name is not a string")
	}

	_path, isOk := volumeRow["path"]
	if !isOk {
		return Volume{}, fmt.Errorf("volume path is missing")
	}

	path, err := typing.AssertType[string](_path)
	if err != nil {
		return Volume{}, fmt.Errorf("volume path is not a string")
	}

	return Volume{
		name: volName,
		path: path,
	}, nil
}

func (v Volume) ShouldDelete() bool {
	return ddl.ShouldDeleteFromName(strings.TrimSuffix(v.name, ".csv"))
}

func (v Volume) Path() string {
	return v.path
}
