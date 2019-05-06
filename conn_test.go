package beanstalk

import (
	"testing"
	"time"
)

func TestNameTooLong(t *testing.T) {
	c := NewConn(mock("", ""))

	tube := Tube{c, string(make([]byte, 201))}
	_, err := tube.Put([]byte("foo"), 0, 0, 0)
	if e, ok := err.(NameError); !ok || e.Err != ErrTooLong {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNameEmpty(t *testing.T) {
	c := NewConn(mock("", ""))

	tube := Tube{c, ""}
	_, err := tube.Put([]byte("foo"), 0, 0, 0)
	if e, ok := err.(NameError); !ok || e.Err != ErrEmpty {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNameBadChar(t *testing.T) {
	c := NewConn(mock("", ""))

	tube := Tube{c, "*"}
	_, err := tube.Put([]byte("foo"), 0, 0, 0)
	if e, ok := err.(NameError); !ok || e.Err != ErrBadChar {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteMissing(t *testing.T) {
	c := NewConn(mock("delete 1\r\n", "NOT_FOUND\r\n"))

	err := c.Delete(1)
	if e, ok := err.(ConnError); !ok || e.Err != ErrNotFound {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUse(t *testing.T) {
	c := NewConn(mock(
		"use foo\r\nput 0 0 0 5\r\nhello\r\n",
		"USING foo\r\nINSERTED 1\r\n",
	))
	tube := Tube{c, "foo"}
	id, err := tube.Put([]byte("hello"), 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatal("expected 1, got", id)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchIgnore(t *testing.T) {
	c := NewConn(mock(
		"watch foo\r\nignore default\r\nreserve-with-timeout 1\r\n",
		"WATCHING 2\r\nWATCHING 1\r\nRESERVED 1 1\r\nx\r\n",
	))
	ts := NewTubeSet(c, "foo")
	id, body, err := ts.Reserve(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatal("expected 1, got", id)
	}
	if len(body) != 1 || body[0] != 'x' {
		t.Fatalf("bad body, expected %#v, got %#v", "x", string(body))
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBury(t *testing.T) {
	c := NewConn(mock("bury 1 3\r\n", "BURIED\r\n"))

	err := c.Bury(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTubeKickJob(t *testing.T) {
	c := NewConn(mock("kick-job 3\r\n", "KICKED\r\n"))

	err := c.KickJob(3)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	c := NewConn(mock("delete 1\r\n", "DELETED\r\n"))

	err := c.Delete(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestListTubes(t *testing.T) {
	c := NewConn(mock("list-tubes\r\n", "OK 14\r\n---\n- default\n\r\n"))

	l, err := c.ListTubes()
	if err != nil {
		t.Fatal(err)
	}
	if len(l) != 1 || l[0] != "default" {
		t.Fatalf("expected %#v, got %#v", []string{"default"}, l)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPeek(t *testing.T) {
	c := NewConn(mock("peek 1\r\n", "FOUND 1 1\r\nx\r\n"))

	body, err := c.Peek(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 1 || body[0] != 'x' {
		t.Fatalf("bad body, expected %#v, got %#v", "x", string(body))
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPeekTwice(t *testing.T) {
	c := NewConn(mock(
		"peek 1\r\npeek 1\r\n",
		"FOUND 1 1\r\nx\r\nFOUND 1 1\r\nx\r\n",
	))

	body, err := c.Peek(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 1 || body[0] != 'x' {
		t.Fatalf("bad body, expected %#v, got %#v", "x", string(body))
	}

	body, err = c.Peek(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 1 || body[0] != 'x' {
		t.Fatalf("bad body, expected %#v, got %#v", "x", string(body))
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRelease(t *testing.T) {
	c := NewConn(mock("release 1 3 2\r\n", "RELEASED\r\n"))

	err := c.Release(1, 3, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStats(t *testing.T) {
	c := NewConn(mock("stats\r\n", "OK 924\r\n---\n"+
		"current-jobs-urgent: 2\n"+
		"current-jobs-ready: 4\n"+
		"current-jobs-reserved: 1\n"+
		"current-jobs-delayed: 1\n"+
		"current-jobs-buried: 1\n"+
		"cmd-put: 11\n"+
		"cmd-peek: 1\n"+
		"cmd-peek-ready: 143\n"+
		"cmd-peek-delayed: 132\n"+
		"cmd-peek-buried: 132\n"+
		"cmd-reserve: 1\n"+
		"cmd-reserve-with-timeout: 1\n"+
		"cmd-delete: 7\n"+
		"cmd-release: 1\n"+
		"cmd-use: 31\n"+
		"cmd-watch: 1\n"+
		"cmd-ignore: 1\n"+
		"cmd-bury: 1\n"+
		"cmd-kick: 1\n"+
		"cmd-touch: 1\n"+
		"cmd-stats: 51\n"+
		"cmd-stats-job: 157\n"+
		"cmd-stats-tube: 7203\n"+
		"cmd-list-tubes: 2298\n"+
		"cmd-list-tube-used: 1\n"+
		"cmd-list-tubes-watched: 1\n"+
		"cmd-pause-tube: 1\n"+
		"job-timeouts: 1\n"+
		"total-jobs: 11\n"+
		"max-job-size: 65535\n"+
		"current-tubes: 3\n"+
		"current-connections: 1\n"+
		"current-producers: 1\n"+
		"current-workers: 1\n"+
		"current-waiting: 1\n"+
		"total-connections: 3600\n"+
		"pid: 1\n"+
		"version: abcd\n"+
		"rusage-utime: 0.464000\n"+
		"rusage-stime: 1.332000\n"+
		"uptime: 1027197\n"+
		"binlog-oldest-index: 1\n"+
		"binlog-current-index: 1\n"+
		"binlog-records-migrated: 1\n"+
		"binlog-records-written: 1\n"+
		"binlog-max-size: 10485760\n"+
		"id: d6643fa880016589\n"+
		"hostname: 8e21001eb759\n\r\n"))

	s, err := c.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if s.CurrentJobsUrgent != 2 ||
		s.CurrentJobsReady != 4 ||
		s.CurrentJobsReserved != 1 ||
		s.CurrentJobsDelayed != 1 ||
		s.CurrentJobsBuried != 1 ||
		s.CmdPut != 11 ||
		s.CmdPeek != 1 ||
		s.CmdPeekReady != 143 ||
		s.CmdPeekDelayed != 132 ||
		s.CmdPeekBuried != 132 ||
		s.CmdReserve != 1 ||
		s.CmdReserveWithTimeout != 1 ||
		s.CmdDelete != 7 ||
		s.CmdRelease != 1 ||
		s.CmdUse != 31 ||
		s.CmdWatch != 1 ||
		s.CmdIgnore != 1 ||
		s.CmdBury != 1 ||
		s.CmdKick != 1 ||
		s.CmdTouch != 1 ||
		s.CmdStats != 51 ||
		s.CmdStatsJob != 157 ||
		s.CmdStatsTube != 7203 ||
		s.CmdListTubes != 2298 ||
		s.CmdListTubeUsed != 1 ||
		s.CmdListTubesWatched != 1 ||
		s.CmdPauseTube != 1 ||
		s.JobTimeouts != 1 ||
		s.TotalJobs != 11 ||
		s.MaxJobSize != 65535 ||
		s.CurrentTubes != 3 ||
		s.CurrentConnections != 1 ||
		s.CurrentProducers != 1 ||
		s.CurrentWorkers != 1 ||
		s.CurrentWaiting != 1 ||
		s.TotalConnections != 3600 ||
		s.Pid != 1 ||
		s.Version != "abcd" ||
		s.RusageUtime != "0.464000" ||
		s.RusageStime != "1.332000" ||
		s.Uptime != 1027197 ||
		s.BinlogOldestIndex != 1 ||
		s.BinlogCurrentIndex != 1 ||
		s.BinlogRecordsMigrated != 1 ||
		s.BinlogRecordsWritten != 1 ||
		s.BinlogMaxSize != 10485760 ||
		s.Id != "d6643fa880016589" ||
		s.Hostname != "8e21001eb759" {
		t.Fatal("got unexpected stats")
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStatsJob(t *testing.T) {
	c := NewConn(mock("stats-job 1\r\n", "OK 148\r\n---\n"+
		"id: 6\n"+
		"tube: default\n"+
		"state: ready\n"+
		"pri: 7\n"+
		"age: 357566\n"+
		"delay: 8\n"+
		"ttr: 9\n"+
		"time-left: 8\n"+
		"file: 7\n"+
		"reserves: 6\n"+
		"timeouts: 5\n"+
		"releases: 4\n"+
		"buries: 3\n"+
		"kicks: 2\n\r\n"))

	s, err := c.StatsJob(1)
	if err != nil {
		t.Fatal(err)
	}
	if s.Id != 6 ||
		s.Tube != "default" ||
		s.State != "ready" ||
		s.Pri != 7 ||
		s.Age != 357566 ||
		s.Delay != 8 ||
		s.Ttr != 9 ||
		s.TimeLeft != 8 ||
		s.File != 7 ||
		s.Reserves != 6 ||
		s.Timeouts != 5 ||
		s.Releases != 4 ||
		s.Buries != 3 ||
		s.Kicks != 2 {
		t.Fatal("got unexpected stats")
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTouch(t *testing.T) {
	c := NewConn(mock("touch 1\r\n", "TOUCHED\r\n"))

	err := c.Touch(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}
