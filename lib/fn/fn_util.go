package fn

func Map[T1, T2 any](in []T1, mappingFn func(T1) T2) []T2 {
	out := make([]T2, len(in))

	for i, s := range in {
		out[i] = mappingFn(s)
	}

	return out
}

func Filter[T any](in []T, fn func(T) bool) []T {
	out := make([]T, 0)

	for _, s := range in {
		if fn(s) {
			out = append(out, s)
		}
	}

	return out
}
