package beanstalk

import (
	"bytes"
	"strconv"
	"strings"
)

const maxSize = uint64(^uint(0) >> 1)

func parseList(dat []byte) []string {
	if dat == nil {
		return nil
	}
	dat = bytes.TrimPrefix(dat, yamlHead)
	lines := strings.Split(string(dat), "\n")
	for i, s := range lines {
		lines[i] = strings.TrimPrefix(s, "- ")
	}
	if l := len(lines); l > 0 && len(lines[l-1]) == 0 {
		lines = lines[:l-1]
	}
	return lines
}

func parseUint(s []byte) (uint64, int) {
	v := uint64(0)
	for k, c := range s {
		if '0' <= c && c <= '9' {
			d := uint64(c - '0')
			if v <= (^uint64(0)-d)/10 {
				v = v*10 + d
				continue
			}
		}
		return v, k
	}
	return v, len(s)
}

func parseSize(s []byte) ([]byte, int, error) {
	i := bytes.LastIndex(s, space)
	if i == -1 {
		return nil, 0, findRespError(string(s))
	}
	b := s[i+1:]
	n, k := parseUint(b)
	if k == 0 || k != len(b) || n > maxSize {
		return nil, 0, unknownRespError(string(s))
	}
	return s[:i], int(n), nil
}

type nonNumericHandler func(string, string)

func parseStats(dat []byte, numIdx map[string]int, nums []uint64, h nonNumericHandler) error {
	dat = bytes.TrimPrefix(dat, yamlHead)
	for lines := string(dat); len(lines) > 0; {
		eol := strings.Index(lines, "\n")
		if eol == -1 {
			return unknownRespError(lines)
		}
		line := lines[:eol]
		lines = lines[eol+1:]
		colon := strings.Index(line, ": ")
		if colon == -1 {
			return unknownRespError(line)
		}
		name, value := line[:colon], line[colon+2:]
		if i, ok := numIdx[name]; ok {
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			nums[i] = n
		} else if h != nil {
			h(name, value)
		}
	}
	return nil
}
