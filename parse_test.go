package beanstalk

import (
	"reflect"
	"testing"
)

func TestParseList(t *testing.T) {
	l := parseList([]byte("---\n- 1\n- 2\n"))
	if !reflect.DeepEqual(l, []string{"1", "2"}) {
		t.Fatalf("got %v", l)
	}
}

func TestParseListEmpty(t *testing.T) {
	l := parseList([]byte{})
	if !reflect.DeepEqual(l, []string{}) {
		t.Fatalf("got %v", l)
	}
}

func TestParseListNil(t *testing.T) {
	l := parseList(nil)
	if l != nil {
		t.Fatalf("got %v", l)
	}
}
