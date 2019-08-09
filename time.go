package beanstalk

import (
	"time"
)

func dur(d time.Duration) uint64 {
	return uint64(d.Seconds())
}
