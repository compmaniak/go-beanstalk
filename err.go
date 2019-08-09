package beanstalk

import (
	"bytes"
	"errors"
)

// ConnError records an error message from the server and the operation
// and connection that caused it.
type ConnError struct {
	Conn *Conn
	Op   string
	Err  error
}

func (e ConnError) Error() string {
	return e.Op + ": " + e.Err.Error()
}

// Error messages returned by the server.
var (
	ErrBadFormat  = errors.New("bad command format")
	ErrBuried     = errors.New("buried")
	ErrDeadline   = errors.New("deadline soon")
	ErrDraining   = errors.New("draining")
	ErrInternal   = errors.New("internal error")
	ErrJobTooBig  = errors.New("job too big")
	ErrNoCRLF     = errors.New("expected CR LF")
	ErrNotFound   = errors.New("not found")
	ErrNotIgnored = errors.New("not ignored")
	ErrOOM        = errors.New("server is out of memory")
	ErrTimeout    = errors.New("timeout")
	ErrUnknown    = errors.New("unknown command")

	resBadFormat  = []byte("BAD_FORMAT")
	resBuried     = []byte("BURIED")
	resDeadline   = []byte("DEADLINE_SOON")
	resDraining   = []byte("DRAINING")
	resInternal   = []byte("INTERNAL_ERROR")
	resJobTooBig  = []byte("JOB_TOO_BIG")
	resNoCRLF     = []byte("EXPECTED_CRLF")
	resNotFound   = []byte("NOT_FOUND")
	resNotIgnored = []byte("NOT_IGNORED")
	resOOM        = []byte("OUT_OF_MEMORY")
	resTimeout    = []byte("TIMED_OUT")
	resUnknown    = []byte("UNKNOWN_COMMAND")
)

type unknownRespError string

func (e unknownRespError) Error() string {
	return "unknown response: " + string(e)
}

func findRespError(s []byte) error {
	if len(s) > 0 {
		ok, err := false, error(nil)
		switch s[0] {
		case 'B':
			ok, err = bytes.Equal(s, resBuried), ErrBuried
			if !ok {
				ok, err = bytes.Equal(s, resBadFormat), ErrBadFormat
			}
		case 'D':
			ok, err = bytes.Equal(s, resDeadline), ErrDeadline
			if !ok {
				ok, err = bytes.Equal(s, resDraining), ErrDraining
			}
		case 'E':
			ok, err = bytes.Equal(s, resNoCRLF), ErrNoCRLF
		case 'I':
			ok, err = bytes.Equal(s, resInternal), ErrInternal
		case 'J':
			ok, err = bytes.Equal(s, resJobTooBig), ErrJobTooBig
		case 'N':
			ok, err = bytes.Equal(s, resNotFound), ErrNotFound
			if !ok {
				ok, err = bytes.Equal(s, resNotIgnored), ErrNotIgnored
			}
		case 'O':
			ok, err = bytes.Equal(s, resOOM), ErrOOM
		case 'T':
			ok, err = bytes.Equal(s, resTimeout), ErrTimeout
		case 'U':
			ok, err = bytes.Equal(s, resUnknown), ErrUnknown
		}
		if ok {
			return err
		}
	}
	return unknownRespError(string(s))
}
