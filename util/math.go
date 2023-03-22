package util

import "golang.org/x/exp/constraints"

// Max returns the maximum of values.
func Max[T constraints.Ordered](values ...T) T {
	max := values[0]
	for _, value := range values {
		if value > max {
			max = value
		}
	}

	return max
}

// Min returns the minimum of values.
func Min[T constraints.Ordered](values ...T) T {
	min := values[0]
	for _, value := range values {
		if value < min {
			min = value
		}
	}

	return min
}
