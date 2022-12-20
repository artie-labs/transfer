package maputil

func GetKeyFromMap(obj map[string]interface{}, key string, defaultValue interface{}) interface{} {
	val, isOk := obj[key]
	if !isOk {
		return defaultValue
	}

	return val
}
