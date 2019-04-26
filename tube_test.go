package beanstalk

import (
	"testing"
	"time"
)

func TestTubePut(t *testing.T) {
	c := NewConn(mock("put 0 0 0 3\r\nfoo\r\n", "INSERTED 1\r\n"))

	id, err := c.Put([]byte("foo"), 0, 0, 0)
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

func TestTubePutBuried(t *testing.T) {
	c := NewConn(mock("put 0 0 0 3\r\nfoo\r\n", "BURIED 7\r\n"))

	id, err := c.Put([]byte("foo"), 0, 0, 0)
	if err == nil {
		t.Fatal("error expected")
	}
	if e, ok := err.(ConnError); !ok || e.Err != ErrBuried {
		t.Fatal("expected ErrBuried, got", err)
	}
	if id != 7 {
		t.Fatal("expected 7, got", id)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTubePeekReady(t *testing.T) {
	c := NewConn(mock("peek-ready\r\n", "FOUND 1 1\r\nx\r\n"))

	id, body, err := c.PeekReady()
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

func TestTubePeekDelayed(t *testing.T) {
	c := NewConn(mock("peek-delayed\r\n", "FOUND 1 1\r\nx\r\n"))

	id, body, err := c.PeekDelayed()
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

func TestTubePeekBuried(t *testing.T) {
	c := NewConn(mock("peek-buried\r\n", "FOUND 1 1\r\nx\r\n"))

	id, body, err := c.PeekBuried()
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

func TestTubeKick(t *testing.T) {
	c := NewConn(mock("kick 2\r\n", "KICKED 1\r\n"))

	n, err := c.Kick(2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("expected 1, got", n)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTubeStats(t *testing.T) {
	c := NewConn(mock("stats-tube default\r\n", "OK 265\r\n---\n"+
		"name: default\n"+
		"current-jobs-urgent: 1\n"+
		"current-jobs-ready: 2\n"+
		"current-jobs-reserved: 3\n"+
		"current-jobs-delayed: 4\n"+
		"current-jobs-buried: 5\n"+
		"total-jobs: 6\n"+
		"current-using: 7\n"+
		"current-waiting: 8\n"+
		"current-watching: 9\n"+
		"cmd-delete: 8\n"+
		"cmd-pause-tube: 7\n"+
		"pause: 6\n"+
		"pause-time-left: 5\n\r\n"))

	s, err := c.Tube.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if s.Name != "default" ||
		s.CurrentJobsUrgent != 1 ||
		s.CurrentJobsReady != 2 ||
		s.CurrentJobsReserved != 3 ||
		s.CurrentJobsDelayed != 4 ||
		s.CurrentJobsBuried != 5 ||
		s.TotalJobs != 6 ||
		s.CurrentUsing != 7 ||
		s.CurrentWaiting != 8 ||
		s.CurrentWatching != 9 ||
		s.CmdDelete != 8 ||
		s.CmdPauseTube != 7 ||
		s.Pause != 6 ||
		s.PauseTimeLeft != 5 {
		t.Fatal("got unexpected stats")
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTubePause(t *testing.T) {
	c := NewConn(mock("pause-tube default 5\r\n", "PAUSED\r\n"))

	err := c.Pause(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}
