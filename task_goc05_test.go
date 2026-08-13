package gocron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTaskGOC05CallJobFuncWithParamsMismatchReturnsError covers the internal
// reflect-based invocation boundary directly: when the number of supplied
// parameters does not match the arity of the job function, the caller must
// receive an explicit error instead of a silent nil (which would make the
// caller believe the job ran successfully).
func TestTaskGOC05CallJobFuncWithParamsMismatchReturnsError(t *testing.T) {
	// Mismatched arity: function takes 2 params, only 1 is supplied.
	err := callJobFuncWithParams(func(_ string, _ int) {}, "one")
	require.Error(t, err, "expected an explicit error for parameter count mismatch, got silent nil")
	require.ErrorContains(t, err, "wrong number of parameters")

	// Control: a matching arity call must still succeed and actually invoke
	// the function (guards against the test failing for unrelated reasons).
	called := false
	err = callJobFuncWithParams(func(_ string) { called = true }, "one")
	require.NoError(t, err)
	require.True(t, called, "matching-arity call must invoke the function")
}

// TestTaskGOC05GlobalJobOptionsMultipleCallsAppend covers the public
// behavior of WithGlobalJobOptions: multiple calls must accumulate (append)
// instead of the later call silently overwriting the earlier one, so every
// globally configured option is applied to each new job.
func TestTaskGOC05GlobalJobOptionsMultipleCallsAppend(t *testing.T) {
	defer verifyNoGoroutineLeaks(t)

	s := newTestScheduler(t,
		WithGlobalJobOptions(WithTags("shared-tag")),
		WithGlobalJobOptions(WithName("shared-name")),
	)

	j, err := s.NewJob(
		DurationJob(time.Hour),
		NewTask(func() {}),
	)
	require.NoError(t, err)

	// If WithGlobalJobOptions overwrote earlier calls, only the options from
	// the last call would be present and Tags() would be empty.
	require.Equal(t, []string{"shared-tag"}, j.Tags())
	require.Equal(t, "shared-name", j.Name())

	require.NoError(t, s.Shutdown())
}
