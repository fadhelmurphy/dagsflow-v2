package dag

// State is a task-instance state (same names as Airflow).
type State string

const (
	StateNone           State = ""
	StateQueued         State = "queued"
	StateRunning        State = "running"
	StateUpForRetry     State = "up_for_retry"
	StateUpForResched   State = "up_for_reschedule"
	StateSuccess        State = "success"
	StateFailed         State = "failed"
	StateSkipped        State = "skipped"
	StateUpstreamFailed State = "upstream_failed"
)

func (s State) String() string {
	if s == StateNone {
		return "none"
	}
	return string(s)
}

func (s State) Finished() bool {
	switch s {
	case StateSuccess, StateFailed, StateSkipped, StateUpstreamFailed:
		return true
	}
	return false
}

type TriggerRule string

const (
	AllSuccess              TriggerRule = "all_success"
	AllFailed               TriggerRule = "all_failed"
	AllDone                 TriggerRule = "all_done"
	OneSuccess              TriggerRule = "one_success"
	OneFailed               TriggerRule = "one_failed"
	NoneFailed              TriggerRule = "none_failed"
	NoneFailedMinOneSuccess TriggerRule = "none_failed_min_one_success"
	NoneSkipped             TriggerRule = "none_skipped"
	Always                  TriggerRule = "always" // runs immediately, does not wait for upstream
)

type Decision int

const (
	Wait Decision = iota
	Run
	Skip
	MarkUpstreamFailed
)

// Evaluate decides what to do with a task given its upstream states,
// following Airflow's trigger-rule semantics (including skip propagation).
func (r TriggerRule) Evaluate(up []State) Decision {
	n := len(up)
	if n == 0 || r == Always {
		return Run
	}
	var succ, fail, skip, done int
	for _, s := range up {
		switch s {
		case StateSuccess:
			succ++
		case StateFailed, StateUpstreamFailed:
			fail++
		case StateSkipped:
			skip++
		}
		if s.Finished() {
			done++
		}
	}
	allDone := done == n

	switch r {
	case AllFailed:
		if succ > 0 || skip > 0 {
			return Skip
		}
		if fail == n {
			return Run
		}
	case AllDone:
		if allDone {
			return Run
		}
	case OneSuccess:
		if succ > 0 {
			return Run
		}
		if allDone {
			if skip == n {
				return Skip
			}
			return MarkUpstreamFailed
		}
	case OneFailed:
		if fail > 0 {
			return Run
		}
		if allDone {
			return Skip
		}
	case NoneFailed:
		if fail > 0 {
			return MarkUpstreamFailed
		}
		if allDone {
			return Run
		}
	case NoneFailedMinOneSuccess:
		if fail > 0 {
			return MarkUpstreamFailed
		}
		if allDone {
			if succ > 0 {
				return Run
			}
			return Skip
		}
	case NoneSkipped:
		if skip > 0 {
			return Skip
		}
		if allDone {
			return Run
		}
	default: // AllSuccess
		if fail > 0 {
			return MarkUpstreamFailed
		}
		if skip > 0 {
			return Skip
		}
		if succ == n {
			return Run
		}
	}
	return Wait
}
