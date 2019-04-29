package beanstalk

import (
	"bytes"
	"strconv"
	"strings"
)

func parseDict(dat []byte) map[string]string {
	if dat == nil {
		return nil
	}
	d := make(map[string]string)
	if bytes.HasPrefix(dat, yamlHead) {
		dat = dat[4:]
	}
	for _, s := range bytes.Split(dat, nl) {
		kv := bytes.SplitN(s, colonSpace, 2)
		if len(kv) != 2 {
			continue
		}
		d[string(kv[0])] = string(kv[1])
	}
	return d
}

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
