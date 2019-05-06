package beanstalk

import (
	"fmt"
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
	nTubeStatsCurrentJobsUrgent int = iota
	nTubeStatsCurrentJobsReady
	nTubeStatsCurrentJobsReserved
	nTubeStatsCurrentJobsDelayed
	nTubeStatsCurrentJobsBuried
	nTubeStatsTotalJobs
	nTubeStatsCurrentUsing
	nTubeStatsCurrentWaiting
	nTubeStatsCurrentWatching
	nTubeStatsCmdDelete
	nTubeStatsCmdPauseTube
	nTubeStatsPause
	nTubeStatsPauseTimeLeft
	nTubeStats
)

var tubeStatToIdx = map[string]int{
	"current-jobs-urgent":   nTubeStatsCurrentJobsUrgent,
	"current-jobs-ready":    nTubeStatsCurrentJobsReady,
	"current-jobs-reserved": nTubeStatsCurrentJobsReserved,
	"current-jobs-delayed":  nTubeStatsCurrentJobsDelayed,
	"current-jobs-buried":   nTubeStatsCurrentJobsBuried,
	"total-jobs":            nTubeStatsTotalJobs,
	"current-using":         nTubeStatsCurrentUsing,
	"current-waiting":       nTubeStatsCurrentWaiting,
	"current-watching":      nTubeStatsCurrentWatching,
	"cmd-delete":            nTubeStatsCmdDelete,
	"cmd-pause-tube":        nTubeStatsCmdPauseTube,
	"pause":                 nTubeStatsPause,
	"pause-time-left":       nTubeStatsPauseTimeLeft,
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
	var stats [nTubeStats]uint64
	err = parseStats(body, tubeStatToIdx, stats[:], nil)
	if err != nil {
		return TubeStats{}, err
	}
	return TubeStats{
		Name:                t.Name,
		CurrentJobsUrgent:   stats[nTubeStatsCurrentJobsUrgent],
		CurrentJobsReady:    stats[nTubeStatsCurrentJobsReady],
		CurrentJobsReserved: stats[nTubeStatsCurrentJobsReserved],
		CurrentJobsDelayed:  stats[nTubeStatsCurrentJobsDelayed],
		CurrentJobsBuried:   stats[nTubeStatsCurrentJobsBuried],
		TotalJobs:           stats[nTubeStatsTotalJobs],
		CurrentUsing:        stats[nTubeStatsCurrentUsing],
		CurrentWaiting:      stats[nTubeStatsCurrentWaiting],
		CurrentWatching:     stats[nTubeStatsCurrentWatching],
		CmdDelete:           stats[nTubeStatsCmdDelete],
		CmdPauseTube:        stats[nTubeStatsCmdPauseTube],
		Pause:               stats[nTubeStatsPause],
		PauseTimeLeft:       stats[nTubeStatsPauseTimeLeft],
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
