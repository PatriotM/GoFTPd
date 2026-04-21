package timeutil

import (
	"strings"
	"sync/atomic"
	"time"

	_ "time/tzdata"
)

var current atomic.Value

func init() {
	current.Store(time.Local)
}

func Set(name string) error {
	name = strings.TrimSpace(name)
	if name == "" || strings.EqualFold(name, "local") {
		current.Store(time.Local)
		return nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return err
	}
	current.Store(loc)
	return nil
}

func Location() *time.Location {
	loc, _ := current.Load().(*time.Location)
	if loc == nil {
		return time.Local
	}
	return loc
}

func Now() time.Time {
	return time.Now().In(Location())
}

func Unix(sec int64) time.Time {
	return time.Unix(sec, 0).In(Location())
}

func In(t time.Time) time.Time {
	return t.In(Location())
}

func FTPMachine(t time.Time) string {
	return t.UTC().Format("20060102150405")
}

func FTPMachineUnix(sec int64) string {
	return FTPMachine(time.Unix(sec, 0))
}
