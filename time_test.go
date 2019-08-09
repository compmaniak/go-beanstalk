package beanstalk

import (
	"testing"
	"time"
)

func TestFormatDuration(t *testing.T) {
	if s := dur(time.Duration(100e9)); s != 100 {
		t.Fatal("got", s, "expected 100")
	}
}
