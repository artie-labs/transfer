package ptr

import "time"

func ToString(val string) *string {
	return &val
}

func ToInt(val int) *int {
	return &val
}

func ToInt32(val int32) *int32 {
	return &val
}

func ToInt64(val int64) *int64 {
	return &val
}

func ToBool(val bool) *bool {
	return &val
}

func ToDuration(duration time.Duration) *time.Duration {
	return &duration
}
