package beanstalk

import (
	"bytes"
	"strconv"
	"strings"
)

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

func parseSize(s string) (string, int, error) {
	i := strings.LastIndex(s, " ")
	if i == -1 {
		return "", 0, findRespError(s)
	}
	n, err := strconv.Atoi(s[i+1:])
	if err != nil {
		return "", 0, err
	}
	return s[:i], n, nil
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
