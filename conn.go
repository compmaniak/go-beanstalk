package beanstalk

import (
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"time"
)

// DefaultDialTimeout is the time to wait for a connection to the beanstalk server.
const DefaultDialTimeout = 10 * time.Second

// DefaultKeepAlivePeriod is the default period between TCP keepalive messages.
const DefaultKeepAlivePeriod = 10 * time.Second

// A Conn represents a connection to a beanstalkd server. It consists
// of a default Tube and TubeSet as well as the underlying network
// connection. The embedded types carry methods with them; see the
// documentation of those types for details.
type Conn struct {
	c       *textproto.Conn
	used    string
	watched map[string]bool
	Tube
	TubeSet
}

type Stats struct {
	CurrentJobsUrgent     uint64
	CurrentJobsReady      uint64
	CurrentJobsReserved   uint64
	CurrentJobsDelayed    uint64
	CurrentJobsBuried     uint64
	CmdPut                uint64
	CmdPeek               uint64
	CmdPeekReady          uint64
	CmdPeekDelayed        uint64
	CmdPeekBuried         uint64
	CmdReserve            uint64
	CmdReserveWithTimeout uint64
	CmdDelete             uint64
	CmdRelease            uint64
	CmdUse                uint64
	CmdWatch              uint64
	CmdIgnore             uint64
	CmdBury               uint64
	CmdKick               uint64
	CmdTouch              uint64
	CmdStats              uint64
	CmdStatsJob           uint64
	CmdStatsTube          uint64
	CmdListTubes          uint64
	CmdListTubeUsed       uint64
	CmdListTubesWatched   uint64
	CmdPauseTube          uint64
	JobTimeouts           uint64
	TotalJobs             uint64
	MaxJobSize            uint64
	CurrentTubes          uint64
	CurrentConnections    uint64
	CurrentProducers      uint64
	CurrentWorkers        uint64
	CurrentWaiting        uint64
	TotalConnections      uint64
	Pid                   uint64
	Version               string
	RusageUtime           string
	RusageStime           string
	Uptime                uint64
	BinlogOldestIndex     uint64
	BinlogCurrentIndex    uint64
	BinlogRecordsMigrated uint64
	BinlogRecordsWritten  uint64
	BinlogMaxSize         uint64
	Id                    string
	Hostname              string
}

type JobStats struct {
	Id       uint64
	Tube     string
	State    string
	Pri      uint64
	Age      uint64
	Delay    uint64
	Ttr      uint64
	TimeLeft uint64
	File     uint64
	Reserves uint64
	Timeouts uint64
	Releases uint64
	Buries   uint64
	Kicks    uint64
}

const (
	nStatsCurrentJobsUrgent int = iota
	nStatsCurrentJobsReady
	nStatsCurrentJobsReserved
	nStatsCurrentJobsDelayed
	nStatsCurrentJobsBuried
	nStatsCmdPut
	nStatsCmdPeek
	nStatsCmdPeekReady
	nStatsCmdPeekDelayed
	nStatsCmdPeekBuried
	nStatsCmdReserve
	nStatsCmdReserveWithTimeout
	nStatsCmdDelete
	nStatsCmdRelease
	nStatsCmdUse
	nStatsCmdWatch
	nStatsCmdIgnore
	nStatsCmdBury
	nStatsCmdKick
	nStatsCmdTouch
	nStatsCmdStats
	nStatsCmdStatsJob
	nStatsCmdStatsTube
	nStatsCmdListTubes
	nStatsCmdListTubeUsed
	nStatsCmdListTubesWatched
	nStatsCmdPauseTube
	nStatsJobTimeouts
	nStatsTotalJobs
	nStatsMaxJobSize
	nStatsCurrentTubes
	nStatsCurrentConnections
	nStatsCurrentProducers
	nStatsCurrentWorkers
	nStatsCurrentWaiting
	nStatsTotalConnections
	nStatsPid
	nStatsUptime
	nStatsBinlogOldestIndex
	nStatsBinlogCurrentIndex
	nStatsBinlogRecordsMigrated
	nStatsBinlogRecordsWritten
	nStatsBinlogMaxSize
	nStats

	nJobStatsId int = iota
	nJobStatsPri
	nJobStatsAge
	nJobStatsDelay
	nJobStatsTtr
	nJobStatsTimeLeft
	nJobStatsFile
	nJobStatsReserves
	nJobStatsTimeouts
	nJobStatsReleases
	nJobStatsBuries
	nJobStatsKicks
	nJobStats
)

var (
	space      = []byte{' '}
	crnl       = []byte{'\r', '\n'}
	yamlHead   = []byte{'-', '-', '-', '\n'}
	nl         = []byte{'\n'}
	colonSpace = []byte{':', ' '}
	minusSpace = []byte{'-', ' '}

	statToIdx = map[string]int{
		"current-jobs-urgent":      nStatsCurrentJobsUrgent,
		"current-jobs-ready":       nStatsCurrentJobsReady,
		"current-jobs-reserved":    nStatsCurrentJobsReserved,
		"current-jobs-delayed":     nStatsCurrentJobsDelayed,
		"current-jobs-buried":      nStatsCurrentJobsBuried,
		"cmd-put":                  nStatsCmdPut,
		"cmd-peek":                 nStatsCmdPeek,
		"cmd-peek-ready":           nStatsCmdPeekReady,
		"cmd-peek-delayed":         nStatsCmdPeekDelayed,
		"cmd-peek-buried":          nStatsCmdPeekBuried,
		"cmd-reserve":              nStatsCmdReserve,
		"cmd-reserve-with-timeout": nStatsCmdReserveWithTimeout,
		"cmd-delete":               nStatsCmdDelete,
		"cmd-release":              nStatsCmdRelease,
		"cmd-use":                  nStatsCmdUse,
		"cmd-watch":                nStatsCmdWatch,
		"cmd-ignore":               nStatsCmdIgnore,
		"cmd-bury":                 nStatsCmdBury,
		"cmd-kick":                 nStatsCmdKick,
		"cmd-touch":                nStatsCmdTouch,
		"cmd-stats":                nStatsCmdStats,
		"cmd-stats-job":            nStatsCmdStatsJob,
		"cmd-stats-tube":           nStatsCmdStatsTube,
		"cmd-list-tubes":           nStatsCmdListTubes,
		"cmd-list-tube-used":       nStatsCmdListTubeUsed,
		"cmd-list-tubes-watched":   nStatsCmdListTubesWatched,
		"cmd-pause-tube":           nStatsCmdPauseTube,
		"job-timeouts":             nStatsJobTimeouts,
		"total-jobs":               nStatsTotalJobs,
		"max-job-size":             nStatsMaxJobSize,
		"current-tubes":            nStatsCurrentTubes,
		"current-connections":      nStatsCurrentConnections,
		"current-producers":        nStatsCurrentProducers,
		"current-workers":          nStatsCurrentWorkers,
		"current-waiting":          nStatsCurrentWaiting,
		"total-connections":        nStatsTotalConnections,
		"pid":                      nStatsPid,
		"uptime":                   nStatsUptime,
		"binlog-oldest-index":      nStatsBinlogOldestIndex,
		"binlog-current-index":     nStatsBinlogCurrentIndex,
		"binlog-records-migrated":  nStatsBinlogRecordsMigrated,
		"binlog-records-written":   nStatsBinlogRecordsWritten,
		"binlog-max-size":          nStatsBinlogMaxSize,
	}

	jobStatToIdx = map[string]int{
		"id":        nJobStatsId,
		"pri":       nJobStatsPri,
		"age":       nJobStatsAge,
		"delay":     nJobStatsDelay,
		"ttr":       nJobStatsTtr,
		"time-left": nJobStatsTimeLeft,
		"file":      nJobStatsFile,
		"reserves":  nJobStatsReserves,
		"timeouts":  nJobStatsTimeouts,
		"releases":  nJobStatsReleases,
		"buries":    nJobStatsBuries,
		"kicks":     nJobStatsKicks,
	}
)

// NewConn returns a new Conn using conn for I/O.
func NewConn(conn io.ReadWriteCloser) *Conn {
	c := new(Conn)
	c.c = textproto.NewConn(conn)
	c.Tube = Tube{c, "default"}
	c.TubeSet = *NewTubeSet(c, "default")
	c.used = "default"
	c.watched = map[string]bool{"default": true}
	return c
}

// Dial connects addr on the given network using net.DialTimeout
// with a default timeout of 10s and then returns a new Conn for the connection.
func Dial(network, addr string) (*Conn, error) {
	return DialTimeout(network, addr, DefaultDialTimeout)
}

// DialTimeout connects addr on the given network using net.DialTimeout
// with a supplied timeout and then returns a new Conn for the connection.
func DialTimeout(network, addr string, timeout time.Duration) (*Conn, error) {
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: DefaultKeepAlivePeriod,
	}
	c, err := dialer.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return NewConn(c), nil
}

// Close closes the underlying network connection.
func (c *Conn) Close() error {
	return c.c.Close()
}

func (c *Conn) cmd(t *Tube, ts *TubeSet, body []byte, op string, args ...interface{}) (req, error) {
	r := req{c.c.Next(), op}
	c.c.StartRequest(r.id)
	err := c.adjustTubes(t, ts)
	if err != nil {
		return req{}, err
	}
	if body != nil {
		args = append(args, len(body))
	}
	c.printLine(string(op), args...)
	if body != nil {
		c.c.W.Write(body)
		c.c.W.Write(crnl)
	}
	err = c.c.W.Flush()
	if err != nil {
		return req{}, ConnError{c, op, err}
	}
	c.c.EndRequest(r.id)
	return r, nil
}

func (c *Conn) adjustTubes(t *Tube, ts *TubeSet) error {
	if t != nil && t.Name != c.used {
		if err := checkName(t.Name); err != nil {
			return err
		}
		c.printLine("use", t.Name)
		c.used = t.Name
	}
	if ts != nil {
		for s := range ts.Name {
			if !c.watched[s] {
				if err := checkName(s); err != nil {
					return err
				}
				c.printLine("watch", s)
			}
		}
		for s := range c.watched {
			if !ts.Name[s] {
				c.printLine("ignore", s)
			}
		}
		c.watched = make(map[string]bool)
		for s := range ts.Name {
			c.watched[s] = true
		}
	}
	return nil
}

// does not flush
func (c *Conn) printLine(cmd string, args ...interface{}) {
	io.WriteString(c.c.W, cmd)
	for _, a := range args {
		c.c.W.Write(space)
		fmt.Fprint(c.c.W, a)
	}
	c.c.W.Write(crnl)
}

func (c *Conn) readRawResp(r req, readBody bool) (header string, body []byte, err error) {
	c.c.StartResponse(r.id)
	defer c.c.EndResponse(r.id)
	line, err := c.c.ReadLine()
	for strings.HasPrefix(line, "WATCHING ") || strings.HasPrefix(line, "USING ") {
		line, err = c.c.ReadLine()
	}
	if err != nil {
		return "", nil, ConnError{c, r.op, err}
	}
	header = line
	if readBody {
		var size int
		header, size, err = parseSize(header)
		if err != nil {
			return "", nil, ConnError{c, r.op, err}
		}
		body = make([]byte, size+2) // include trailing CR NL
		_, err = io.ReadFull(c.c.R, body)
		if err != nil {
			return header, nil, ConnError{c, r.op, err}
		}
		body = body[:size] // exclude trailing CR NL
	}
	return
}

func (c *Conn) readResp(r req, readBody bool, f string, a ...interface{}) ([]byte, error) {
	header, body, err := c.readRawResp(r, readBody)
	if err != nil {
		return nil, err
	}
	err = scan(header, f, a...)
	if err != nil {
		return nil, ConnError{c, r.op, err}
	}
	return body, nil
}

// Delete deletes the given job.
func (c *Conn) Delete(id uint64) error {
	r, err := c.cmd(nil, nil, nil, "delete", id)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "DELETED")
	return err
}

// Release tells the server to perform the following actions:
// set the priority of the given job to pri, remove it from the list of
// jobs reserved by c, wait delay seconds, then place the job in the
// ready queue, which makes it available for reservation by any client.
func (c *Conn) Release(id uint64, pri uint32, delay time.Duration) error {
	r, err := c.cmd(nil, nil, nil, "release", id, pri, dur(delay))
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "RELEASED")
	return err
}

// Bury places the given job in a holding area in the job's tube and
// sets its priority to pri. The job will not be scheduled again until it
// has been kicked; see also the documentation of Kick.
func (c *Conn) Bury(id uint64, pri uint32) error {
	r, err := c.cmd(nil, nil, nil, "bury", id, pri)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "BURIED")
	return err
}

// KickJob places the given job to the ready queue of the same tube where it currently belongs
// when the given job id exists and is in a buried or delayed state.
func (c *Conn) KickJob(id uint64) error {
	r, err := c.cmd(nil, nil, nil, "kick-job", id)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "KICKED")
	return err
}

// Touch resets the reservation timer for the given job.
// It is an error if the job isn't currently reserved by c.
// See the documentation of Reserve for more details.
func (c *Conn) Touch(id uint64) error {
	r, err := c.cmd(nil, nil, nil, "touch", id)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "TOUCHED")
	return err
}

// Peek gets a copy of the specified job from the server.
func (c *Conn) Peek(id uint64) (body []byte, err error) {
	r, err := c.cmd(nil, nil, nil, "peek", id)
	if err != nil {
		return nil, err
	}
	return c.readResp(r, true, "FOUND %d", &id)
}

func (c *Conn) Stats() (Stats, error) {
	r, err := c.cmd(nil, nil, nil, "stats")
	if err != nil {
		return Stats{}, err
	}
	body, err := c.readResp(r, true, "OK")
	if err != nil {
		return Stats{}, err
	}
	var res Stats
	var stats [nStats]uint64
	err = parseStats(body, statToIdx, stats[:], func(name string, value string) {
		switch name {
		case "version":
			res.Version = value
		case "rusage-utime":
			res.RusageUtime = value
		case "rusage-stime":
			res.RusageStime = value
		case "id":
			res.Id = value
		case "hostname":
			res.Hostname = value
		}
	})
	if err != nil {
		return Stats{}, err
	}
	res.CurrentJobsUrgent = stats[nStatsCurrentJobsUrgent]
	res.CurrentJobsReady = stats[nStatsCurrentJobsReady]
	res.CurrentJobsReserved = stats[nStatsCurrentJobsReserved]
	res.CurrentJobsDelayed = stats[nStatsCurrentJobsDelayed]
	res.CurrentJobsBuried = stats[nStatsCurrentJobsBuried]
	res.CmdPut = stats[nStatsCmdPut]
	res.CmdPeek = stats[nStatsCmdPeek]
	res.CmdPeekReady = stats[nStatsCmdPeekReady]
	res.CmdPeekDelayed = stats[nStatsCmdPeekDelayed]
	res.CmdPeekBuried = stats[nStatsCmdPeekBuried]
	res.CmdReserve = stats[nStatsCmdReserve]
	res.CmdReserveWithTimeout = stats[nStatsCmdReserveWithTimeout]
	res.CmdDelete = stats[nStatsCmdDelete]
	res.CmdRelease = stats[nStatsCmdRelease]
	res.CmdUse = stats[nStatsCmdUse]
	res.CmdWatch = stats[nStatsCmdWatch]
	res.CmdIgnore = stats[nStatsCmdIgnore]
	res.CmdBury = stats[nStatsCmdBury]
	res.CmdKick = stats[nStatsCmdKick]
	res.CmdTouch = stats[nStatsCmdTouch]
	res.CmdStats = stats[nStatsCmdStats]
	res.CmdStatsJob = stats[nStatsCmdStatsJob]
	res.CmdStatsTube = stats[nStatsCmdStatsTube]
	res.CmdListTubes = stats[nStatsCmdListTubes]
	res.CmdListTubeUsed = stats[nStatsCmdListTubeUsed]
	res.CmdListTubesWatched = stats[nStatsCmdListTubesWatched]
	res.CmdPauseTube = stats[nStatsCmdPauseTube]
	res.JobTimeouts = stats[nStatsJobTimeouts]
	res.TotalJobs = stats[nStatsTotalJobs]
	res.MaxJobSize = stats[nStatsMaxJobSize]
	res.CurrentTubes = stats[nStatsCurrentTubes]
	res.CurrentConnections = stats[nStatsCurrentConnections]
	res.CurrentProducers = stats[nStatsCurrentProducers]
	res.CurrentWorkers = stats[nStatsCurrentWorkers]
	res.CurrentWaiting = stats[nStatsCurrentWaiting]
	res.TotalConnections = stats[nStatsTotalConnections]
	res.Pid = stats[nStatsPid]
	res.Uptime = stats[nStatsUptime]
	res.BinlogOldestIndex = stats[nStatsBinlogOldestIndex]
	res.BinlogCurrentIndex = stats[nStatsBinlogCurrentIndex]
	res.BinlogRecordsMigrated = stats[nStatsBinlogRecordsMigrated]
	res.BinlogRecordsWritten = stats[nStatsBinlogRecordsWritten]
	res.BinlogMaxSize = stats[nStatsBinlogMaxSize]
	return res, nil
}

// StatsJob retrieves statistics about the given job.
func (c *Conn) StatsJob(id uint64) (JobStats, error) {
	r, err := c.cmd(nil, nil, nil, "stats-job", id)
	if err != nil {
		return JobStats{}, err
	}
	body, err := c.readResp(r, true, "OK")
	if err != nil {
		return JobStats{}, err
	}
	var res JobStats
	var stats [nJobStats]uint64
	err = parseStats(body, jobStatToIdx, stats[:], func(name string, value string) {
		switch name {
		case "tube":
			res.Tube = value
		case "state":
			res.State = value
		}
	})
	if err != nil {
		return JobStats{}, err
	}
	res.Id = stats[nJobStatsId]
	res.Pri = stats[nJobStatsPri]
	res.Age = stats[nJobStatsAge]
	res.Delay = stats[nJobStatsDelay]
	res.Ttr = stats[nJobStatsTtr]
	res.TimeLeft = stats[nJobStatsTimeLeft]
	res.File = stats[nJobStatsFile]
	res.Reserves = stats[nJobStatsReserves]
	res.Timeouts = stats[nJobStatsTimeouts]
	res.Releases = stats[nJobStatsReleases]
	res.Buries = stats[nJobStatsBuries]
	res.Kicks = stats[nJobStatsKicks]
	return res, nil
}

// ListTubes returns the names of the tubes that currently
// exist on the server.
func (c *Conn) ListTubes() ([]string, error) {
	r, err := c.cmd(nil, nil, nil, "list-tubes")
	if err != nil {
		return nil, err
	}
	body, err := c.readResp(r, true, "OK")
	return parseList(body), err
}

func scan(input, format string, a ...interface{}) error {
	_, err := fmt.Sscanf(input, format, a...)
	if err != nil {
		return findRespError(input)
	}
	return nil
}

type req struct {
	id uint
	op string
}
