package gocron

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

// stubScheduleGOC02 is a test jobSchedule whose next() behavior is fully
// controlled by nextFunc, letting tests simulate exhausted or misbehaving
// schedules (zero / non-advancing next values).
type stubScheduleGOC02 struct {
	nextFunc func(lastRun time.Time) time.Time
}

func (s stubScheduleGOC02) next(lastRun time.Time) time.Time {
	return s.nextFunc(lastRun)
}

// TestTaskGOC02_ExecutionTimeUsesInjectedClock verifies that job timing
// metrics are measured with the scheduler's injected clock rather than the
// wall clock. With a fake clock, a job that advances the clock by a known
// duration during execution must report that same duration as its execution
// time. Prior to the fix, runJob captured start/end via time.Now(), so the
// reported duration reflected wall time (~0) instead of the injected clock.
func TestTaskGOC02_ExecutionTimeUsesInjectedClock(t *testing.T) {
	defer verifyNoGoroutineLeaks(t)

	fakeClock := clockwork.NewFakeClockAt(time.Date(2050, time.January, 1, 0, 0, 0, 0, time.UTC))
	monitor := newTestSchedulerMonitor()

	s := newTestScheduler(t,
		WithClock(fakeClock),
		WithSchedulerMonitor(monitor),
	)

	const jobDuration = 5 * time.Second
	ran := make(chan struct{}, 1)
	_, err := s.NewJob(
		DurationJob(time.Hour),
		NewTask(func() {
			// Simulate work that takes jobDuration on the injected clock.
			fakeClock.Advance(jobDuration)
			select {
			case ran <- struct{}{}:
			default:
			}
		}),
		WithStartAt(WithStartImmediately()),
	)
	require.NoError(t, err)

	s.Start()

	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not run within 2s")
	}

	require.NoError(t, s.Shutdown())

	monitor.mu.RLock()
	execTimes := append([]time.Duration(nil), monitor.jobExecutionTimes...)
	monitor.mu.RUnlock()

	require.NotEmpty(t, execTimes, "expected at least one JobExecutionTime notification")
	// With the injected clock, execution time equals the advanced duration.
	// With the wall-clock bug it would be sub-millisecond.
	require.GreaterOrEqual(t, execTimes[0], jobDuration,
		"execution time must be measured on the injected clock (got %s, want >= %s)", execTimes[0], jobDuration)
}

// TestTaskGOC02_RescheduleLoopConverges verifies that the rescheduling path
// converges (rather than spinning forever) when a schedule's next() returns a
// value that never advances past an already-present nextScheduled entry. The
// reschedule loop previously incremented via next() unconditionally, which
// loops forever when next() returns the same duplicate value (or would arm a
// zero/negative-duration timer that busy-loops the scheduler goroutine).
// A timeout guards the test so the regression is reported instead of hanging.
func TestTaskGOC02_RescheduleLoopConverges(t *testing.T) {
	base := time.Date(2026, time.July, 9, 12, 0, 0, 0, time.UTC)
	// The fake clock sits one hour before base so the duplicate-value branch
	// (not the past-time guard) is the path being exercised.
	fakeClock := clockwork.NewFakeClockAt(base.Add(-time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id := uuid.New()
	j := internalJob{
		ctx:    ctx,
		cancel: cancel,
		id:     id,
		// next() always returns the same value already present in
		// nextScheduled: the reschedule loop must still converge.
		jobSchedule:   stubScheduleGOC02{nextFunc: func(_ time.Time) time.Time { return base }},
		nextScheduled: []time.Time{base},
	}
	s := &scheduler{
		shutdownCtx: context.Background(),
		exec:        executor{clock: fakeClock},
		jobs:        map[uuid.UUID]internalJob{id: j},
		location:    time.UTC,
	}

	done := make(chan struct{})
	go func() {
		s.selectExecJobsOutForRescheduling(id)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reschedule loop did not converge on a non-advancing schedule (regression)")
	}
}
