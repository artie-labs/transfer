package ptr

func ToString(val string) *string {
	return &val
}

func ToInt(val int) *int {
	return &val
}

func ToInt64(val int64) *int64 {
	return &val
}
