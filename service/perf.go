package service

import (
	"time"
)

type StubTimer struct {
	start time.Time
}

func StartTimer() *StubTimer {
	return &StubTimer{
		start: time.Now(),
	}
}

func (s StubTimer) Stop() time.Duration {
	return time.Now().Sub(s.start)
}
