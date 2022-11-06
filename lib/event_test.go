package lib

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetPrimaryKey(t *testing.T) {
	valString := `{"id": 47}`
	pkName, pkVal, err := GetPrimaryKey(context.Background(), []byte(valString))
	assert.Equal(t, pkName, "id")
	assert.Equal(t, fmt.Sprint(pkVal), fmt.Sprint(47)) // Don't have to deal with float and int conversion
	assert.Equal(t, err, nil)
}

func TestGetPrimaryKeyUUID(t *testing.T) {
	valString := `{"uuid": "ca0cefe9-45cf-44fa-a2ab-ec5e7e5522a3"}`
	pkName, pkVal, err := GetPrimaryKey(context.Background(), []byte(valString))
	assert.Equal(t, pkName, "uuid")
	assert.Equal(t, fmt.Sprint(pkVal), "ca0cefe9-45cf-44fa-a2ab-ec5e7e5522a3")
	assert.Equal(t, err, nil)
}
