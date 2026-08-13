package gocron

// This file contains the hidden regression tests for task GOC03.
// They target the public behavior described by the task:
// multiple jobs sharing a JobDefinition whose cron/interval state is
// mutated during setup (initialization), accessed concurrently by the
// scheduler goroutine and external Job.NextRuns callers, producing a
// data race and cross-job state pollution.

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTaskGOC03_SharedCronDefinition_StateIsolation verifies that two
// jobs created from the same JobDefinition keep isolated cron state:
// re-initializing one job (Update) while the scheduler is running and
// while external callers invoke NextRuns must not race on or corrupt
// the other job's schedule state.
func TestTaskGOC03_SharedCronDefinition_StateIsolation(t *testing.T) {
	defer verifyNoGoroutineLeaks(t)

	// A single shared definition reused across two jobs. The definition
	// owns a *defaultCron whose cronSchedule field is written by IsValid
	// during setup; before the fix both jobs aliased that same pointer,
	// so a later setup (Update) mutated state that the scheduler and
	// concurrent Job.NextRuns callers were reading.
	def := CronJob("*/5 * * * *", false)
	s := newTestScheduler(t)

	j1, err := s.NewJob(def, NewTask(func() {}))
	require.NoError(t, err)
	j2, err := s.NewJob(def, NewTask(func() {}))
	require.NoError(t, err)
	s.Start()

	var wg sync.WaitGroup

	// Writer: re-initialize j2 from the same shared definition, which
	// runs the initialization (setup/IsValid) that mutates the shared
	// cron state.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, err := s.Update(j2.ID(), def, NewTask(func() {}))
			require.NoError(t, err)
		}
	}()

	// Readers: external NextRuns concurrent with the scheduler goroutine.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				runs, err := j1.NextRuns(3)
				require.NoError(t, err)
				require.NotEmpty(t, runs)
			}
		}()
	}

	wg.Wait()
	require.NoError(t, s.Shutdown())
}

// TestTaskGOC03_DurationRandomJob_NextRunsConcurrentSafe verifies that
// an interval job with a random duration tolerates Job.NextRuns being
// called concurrently with the scheduler goroutine's own scheduling
// computations (previously a data race on the per-job *rand.Rand).
func TestTaskGOC03_DurationRandomJob_NextRunsConcurrentSafe(t *testing.T) {
	defer verifyNoGoroutineLeaks(t)

	s := newTestScheduler(t)
	j, err := s.NewJob(
		DurationRandomJob(10*time.Millisecond, 20*time.Millisecond),
		NewTask(func() {}),
	)
	require.NoError(t, err)
	s.Start()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 200; k++ {
				runs, err := j.NextRuns(5)
				require.NoError(t, err)
				require.NotEmpty(t, runs)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, s.Shutdown())
}
