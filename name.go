package beanstalk

import (
	"errors"
	"strings"
)

// Characters allowed in a name in the beanstalkd protocol.
const NameChars = `\-+/;.$_()0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz`

// NameError indicates that a name was malformed and the specific error
// describing how.
type NameError struct {
	Name string
	Err  error
}

func (e NameError) Error() string {
	return e.Err.Error() + ": " + e.Name
}

// Name format errors. The Err field of NameError contains one of these.
var (
	ErrEmpty   = errors.New("name is empty")
	ErrBadChar = errors.New("name has bad char") // contains a character not in NameChars
	ErrTooLong = errors.New("name is too long")
)

func CheckName(s string) error {
	switch {
	case len(s) == 0:
		return NameError{s, ErrEmpty}
	case len(s) >= 200:
		return NameError{s, ErrTooLong}
	case !containsOnly(s, NameChars):
		return NameError{s, ErrBadChar}
	}
	return nil
}

func containsOnly(s, chars string) bool {
	for _, c := range s {
		if !strings.ContainsRune(chars, c) {
			return false
		}
	}
	return true
}
