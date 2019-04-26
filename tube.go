package beanstalk

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Tube represents tube Name on the server connected to by Conn.
// It has methods for commands that operate on a single tube.
type Tube struct {
	Conn *Conn
	Name string
}

type TubeStats struct {
	Name                string
	CurrentJobsUrgent   uint64
	CurrentJobsReady    uint64
	CurrentJobsReserved uint64
	CurrentJobsDelayed  uint64
	CurrentJobsBuried   uint64
	TotalJobs           uint64
	CurrentUsing        uint64
	CurrentWaiting      uint64
	CurrentWatching     uint64
	CmdDelete           uint64
	CmdPauseTube        uint64
	Pause               uint64
	PauseTimeLeft       uint64
}

const (
	idxCurrentJobsUrgent int = iota
	idxCurrentJobsReady
	idxCurrentJobsReserved
	idxCurrentJobsDelayed
	idxCurrentJobsBuried
	idxTotalJobs
	idxCurrentUsing
	idxCurrentWaiting
	idxCurrentWatching
	idxCmdDelete
	idxCmdPauseTube
	idxPause
	idxPauseTimeLeft
)

var statNameToIdx = map[string]int{
	"current-jobs-urgent":   idxCurrentJobsUrgent,
	"current-jobs-ready":    idxCurrentJobsReady,
	"current-jobs-reserved": idxCurrentJobsReserved,
	"current-jobs-delayed":  idxCurrentJobsDelayed,
	"current-jobs-buried":   idxCurrentJobsBuried,
	"total-jobs":            idxTotalJobs,
	"current-using":         idxCurrentUsing,
	"current-waiting":       idxCurrentWaiting,
	"current-watching":      idxCurrentWatching,
	"cmd-delete":            idxCmdDelete,
	"cmd-pause-tube":        idxCmdPauseTube,
	"pause":                 idxPause,
	"pause-time-left":       idxPauseTimeLeft,
}

// Put puts a job into tube t with priority pri and TTR ttr, and returns
// the id of the newly-created job. If delay is nonzero, the server will
// wait the given amount of time after returning to the client and before
// putting the job into the ready queue.
func (t *Tube) Put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error) {
	r, err := t.Conn.cmd(t, nil, body, "put", pri, dur(delay), dur(ttr))
	if err != nil {
		return 0, err
	}
	var header string
	header, _, err = t.Conn.readRawResp(r, false)
	if err != nil {
		return 0, err
	}
	_, err = fmt.Sscanf(header, "INSERTED %d", &id)
	if err != nil {
		err = scan(header, "BURIED %d", &id)
		if err == nil {
			err = ConnError{t.Conn, r.op, ErrBuried}
		}
	}
	return id, err
}

// PeekReady gets a copy of the job at the front of t's ready queue.
func (t *Tube) PeekReady() (id uint64, body []byte, err error) {
	r, err := t.Conn.cmd(t, nil, nil, "peek-ready")
	if err != nil {
		return 0, nil, err
	}
	body, err = t.Conn.readResp(r, true, "FOUND %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}

// PeekDelayed gets a copy of the delayed job that is next to be
// put in t's ready queue.
func (t *Tube) PeekDelayed() (id uint64, body []byte, err error) {
	r, err := t.Conn.cmd(t, nil, nil, "peek-delayed")
	if err != nil {
		return 0, nil, err
	}
	body, err = t.Conn.readResp(r, true, "FOUND %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}

// PeekBuried gets a copy of the job in the holding area that would
// be kicked next by Kick.
func (t *Tube) PeekBuried() (id uint64, body []byte, err error) {
	r, err := t.Conn.cmd(t, nil, nil, "peek-buried")
	if err != nil {
		return 0, nil, err
	}
	body, err = t.Conn.readResp(r, true, "FOUND %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}

// Kick takes up to bound jobs from the holding area and moves them into
// the ready queue, then returns the number of jobs moved. Jobs will be
// taken in the order in which they were last buried.
func (t *Tube) Kick(bound int) (n int, err error) {
	r, err := t.Conn.cmd(t, nil, nil, "kick", bound)
	if err != nil {
		return 0, err
	}
	_, err = t.Conn.readResp(r, false, "KICKED %d", &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Stats retrieves statistics about tube t.
func (t *Tube) Stats() (TubeStats, error) {
	r, err := t.Conn.cmd(nil, nil, nil, "stats-tube", t.Name)
	if err != nil {
		return TubeStats{}, err
	}
	body, err := t.Conn.readResp(r, true, "OK")
	if err != nil {
		return TubeStats{}, err
	}
	var stats [13]uint64
	body = bytes.TrimPrefix(body, yamlHead)
	for lines := string(body); len(lines) > 0; {
		eol := strings.Index(lines, "\n")
		if eol == -1 {
			return TubeStats{}, unknownRespError(lines)
		}
		line := lines[:eol]
		lines = lines[eol+1:]
		colon := strings.Index(line, ": ")
		if colon == -1 {
			return TubeStats{}, unknownRespError(line)
		}
		i, ok := statNameToIdx[line[:colon]]
		if ok {
			v, err := strconv.ParseUint(line[colon+2:], 10, 64)
			if err != nil {
				return TubeStats{}, err
			}
			stats[i] = v
		}
	}
	return TubeStats{
		Name:                t.Name,
		CurrentJobsUrgent:   stats[idxCurrentJobsUrgent],
		CurrentJobsReady:    stats[idxCurrentJobsReady],
		CurrentJobsReserved: stats[idxCurrentJobsReserved],
		CurrentJobsDelayed:  stats[idxCurrentJobsDelayed],
		CurrentJobsBuried:   stats[idxCurrentJobsBuried],
		TotalJobs:           stats[idxTotalJobs],
		CurrentUsing:        stats[idxCurrentUsing],
		CurrentWaiting:      stats[idxCurrentWaiting],
		CurrentWatching:     stats[idxCurrentWatching],
		CmdDelete:           stats[idxCmdDelete],
		CmdPauseTube:        stats[idxCmdPauseTube],
		Pause:               stats[idxPause],
		PauseTimeLeft:       stats[idxPauseTimeLeft],
	}, nil
}

// Pause pauses new reservations in t for time d.
func (t *Tube) Pause(d time.Duration) error {
	r, err := t.Conn.cmd(nil, nil, nil, "pause-tube", t.Name, dur(d))
	if err != nil {
		return err
	}
	_, err = t.Conn.readResp(r, false, "PAUSED")
	if err != nil {
		return err
	}
	return nil
}
