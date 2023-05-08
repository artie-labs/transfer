package size

import (
	"fmt"
)

// GetApproxSize will encode the actual variable into bytes and then check the length (approx) by using string encoding.
// We chose not to use unsafe.SizeOf or reflect.Type.Size (both are akin) because they do not do recursive traversal.
// We also chose not to use gob.NewEncoder because it does not work for all data types and had a huge computational overhead.
// Another bonus is that there is no possible way for this to error out.
func GetApproxSize(v interface{}) int {
	valString := fmt.Sprint(v)
	return len([]byte(valString))
}
