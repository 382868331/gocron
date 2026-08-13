package gocron

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// goc01SchedulingDelayRecorder records every JobSchedulingDelay
// notification issued by the scheduler monitor, keeping the raw
// (scheduledTime, actualStartTime) pair so the test can assert on the
// scheduled time that was attributed to a run.
type goc01SchedulingDelayRecorder struct {
	mu    sync.Mutex
	calls []goc01DelayCall
}

type goc01DelayCall struct {
	scheduledTime   time.Time
	actualStartTime time.Time
}

func (r *goc01SchedulingDelayRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

func (r *goc01SchedulingDelayRecorder) snapshot() []goc01DelayCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]goc01DelayCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// SchedulerMonitor implementation: only JobSchedulingDelay is of
// interest; the remaining callbacks are no-ops.
func (r *goc01SchedulingDelayRecorder) SchedulerStarted()                          {}
func (r *goc01SchedulingDelayRecorder) SchedulerStopped()                          {}
func (r *goc01SchedulingDelayRecorder) SchedulerShutdown()                         {}
func (r *goc01SchedulingDelayRecorder) JobRegistered(Job)                          {}
func (r *goc01SchedulingDelayRecorder) JobUnregistered(Job)                        {}
func (r *goc01SchedulingDelayRecorder) JobStarted(Job)                             {}
func (r *goc01SchedulingDelayRecorder) JobRunning(Job)                             {}
func (r *goc01SchedulingDelayRecorder) JobFailed(Job, error)                       {}
func (r *goc01SchedulingDelayRecorder) JobCompleted(Job)                           {}
func (r *goc01SchedulingDelayRecorder) JobExecutionTime(Job, time.Duration)        {}
func (r *goc01SchedulingDelayRecorder) ConcurrencyLimitReached(string, Job)        {}

func (r *goc01SchedulingDelayRecorder) JobSchedulingDelay(_ Job, scheduledTime time.Time, actualStartTime time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, goc01DelayCall{scheduledTime: scheduledTime, actualStartTime: actualStartTime})
}

// TestTaskGOC01SchedulingDelayAttributesFiredTick is the regression
// test for the scheduling-delay attribution bug: when the job's
// pending-tick list (internalJob.nextScheduled) holds more than one
// entry, the scheduling delay monitor must be attributed to the tick
// whose run actually started — the latest entry that is not later than
// the actual start time — instead of always using the earliest entry
// (index 0). Using the earliest entry overstates the measured delay for
// the run.
func TestTaskGOC01SchedulingDelayAttributesFiredTick(t *testing.T) {
	defer verifyNoGoroutineLeaks(t)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rec := &goc01SchedulingDelayRecorder{}

	run := func(name string, nextScheduled []time.Time, actualStart time.Time, want time.Time) {
		t.Run(name, func(t *testing.T) {
			// Each subtest gets its own scheduler whose clock is
			// positioned exactly at the run's actual start time.
			fakeClock := clockwork.NewFakeClockAt(actualStart)
			s, err := NewScheduler(WithClock(fakeClock), WithSchedulerMonitor(rec))
			require.NoError(t, err)
			defer func() { require.NoError(t, s.Shutdown()) }()

			exec := s.(*scheduler).exec
			// executor.start() normally owns this context; provide one
			// so runJob can be driven directly without starting the
			// full scheduler loop.
			exec.ctx = context.Background()

			rec.reset()

			job := internalJob{
				ctx:           context.Background(),
				id:            uuid.New(),
				name:          "goc01-job",
				nextScheduled: nextScheduled,
				function:      func() {},
			}
			exec.runJob(job, jobIn{id: job.id})

			calls := rec.snapshot()
			require.Len(t, calls, 1, "exactly one JobSchedulingDelay notification is expected for the run")
			assert.True(t, calls[0].actualStartTime.Equal(actualStart),
				"actualStartTime: got %v, want %v", calls[0].actualStartTime, actualStart)
			assert.True(t, calls[0].scheduledTime.Equal(want),
				"reported scheduledTime should be the tick that actually fired (%v), got %v (earliest pending tick)",
				want, calls[0].scheduledTime)
		})
	}

	older := base.Add(-2 * time.Hour)
	fired := base.Add(-1 * time.Hour)

	// A single pending tick keeps being reported unchanged.
	run("single pending tick keeps reporting that tick",
		[]time.Time{fired}, base.Add(5*time.Second), fired)

	// Core regression: with multiple pending ticks the reported
	// scheduled time must be the tick the run corresponds to (the
	// latest tick at-or-before the actual start), not index 0.
	run("multiple pending ticks report the fired tick not index 0",
		[]time.Time{older, fired}, base, fired)

	// Exact match: the run starts exactly on one of the pending ticks.
	run("exact match reports the matching tick",
		[]time.Time{older, fired}, fired, fired)
}
