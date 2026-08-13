package gocron

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestTaskGOC04_BeforeFuncErrorSkipsMustNotConsumeLimitedRuns verifies that
// scheduled invocations aborted by the before hook (BeforeJobRunsSkipIfBeforeFuncErrors
// returning an error) do NOT consume the WithLimitedRuns budget: the task body must
// still execute exactly `runLimit` times before the job is removed. This distinguishes
// "scheduled attempts" from "actual executions".
func TestTaskGOC04_BeforeFuncErrorSkipsMustNotConsumeLimitedRuns(t *testing.T) {
	s, err := NewScheduler()
	require.NoError(t, err)

	var skipCalls atomic.Int32
	var taskRuns atomic.Int32
	const forcedSkips = 2
	const runLimit = 3

	_, err = s.NewJob(
		DurationJob(20*time.Millisecond),
		NewTask(func() { taskRuns.Add(1) }),
		WithStartAt(WithStartImmediately()),
		WithLimitedRuns(runLimit),
		WithEventListeners(
			BeforeJobRunsSkipIfBeforeFuncErrors(func(_ uuid.UUID, _ string) error {
				if skipCalls.Add(1) <= forcedSkips {
					return errors.New("forced skip")
				}
				return nil
			}),
		),
	)
	require.NoError(t, err)

	s.Start()
	// Long enough for the forced skips plus runLimit real runs at a 20ms
	// interval to play out, with margin for CI noise.
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, s.Shutdown())

	require.GreaterOrEqual(t, skipCalls.Load(), int32(forcedSkips),
		"the before hook should have fired at least forcedSkips times")
	require.Equal(t, int32(runLimit), taskRuns.Load(),
		"the task body must execute exactly WithLimitedRuns(3) times; "+
			"invocations skipped by the before hook must not consume the limited-runs budget")
}
