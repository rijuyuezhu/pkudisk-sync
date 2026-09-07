package reconcile

import "fmt"

// RecoveryOutcome describes what observation can prove about a previously
// running external mutation after a crash/restart.
type RecoveryOutcome string

const (
	RecoveryPostconditionSatisfied RecoveryOutcome = "postcondition-satisfied"
	RecoveryNotApplied             RecoveryOutcome = "not-applied"
	RecoveryAmbiguous              RecoveryOutcome = "ambiguous"
	RecoveryDiverged               RecoveryOutcome = "diverged"
)

// RecoveryAction is the safe next step. Retry is emitted only when observation
// proves the mutation did not happen and the original preconditions still hold.
type RecoveryAction string

const (
	RecoveryCommit    RecoveryAction = "commit"
	RecoveryRetry     RecoveryAction = "retry"
	RecoveryReconcile RecoveryAction = "reconcile"
	RecoveryBlock     RecoveryAction = "block"
)

// RecoveryObservation is deliberately executor-agnostic. Phase C is
// responsible for observing operation-specific pre/postconditions and reducing
// them to these facts.
type RecoveryObservation struct {
	Outcome           RecoveryOutcome
	PreconditionsHold bool
}

func DecideRecovery(observation RecoveryObservation) (RecoveryAction, error) {
	switch observation.Outcome {
	case RecoveryPostconditionSatisfied:
		return RecoveryCommit, nil
	case RecoveryNotApplied:
		if observation.PreconditionsHold {
			return RecoveryRetry, nil
		}
		return RecoveryReconcile, nil
	case RecoveryAmbiguous:
		return RecoveryReconcile, nil
	case RecoveryDiverged:
		return RecoveryBlock, nil
	default:
		return "", fmt.Errorf("invalid recovery outcome %q", observation.Outcome)
	}
}
