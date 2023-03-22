package util

import (
	"time"
)

func Run(initBackoff float64,
	maxBackoff float64,
	maxAttempts int,
	f func() (data any, cancel bool, err error)) (any, bool, error) {
	var (
		res    any
		cancel bool
		cause  error
	)
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			time.Sleep(RandBackoffSeconds(initBackoff, maxBackoff, 2.0, i))
		}

		res, cancel, cause = f()
		if cause == nil || cancel {
			break
		}
	}

	return res, cancel, cause
}
