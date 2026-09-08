package reconcile

import "fmt"

// DeletePolicy is intentionally supplied by the caller rather than hiding
// product defaults in the pure engine. Zero disables an individual threshold;
// at least one threshold is required before automatic deletes are allowed.
type DeletePolicy struct {
	MaxCount           int
	MaxFraction        float64
	MassDeleteApproved bool
}

func (p DeletePolicy) validate() error {
	if p.MaxCount < 0 {
		return fmt.Errorf("max delete count must be non-negative")
	}
	if p.MaxFraction < 0 || p.MaxFraction > 1 {
		return fmt.Errorf("max delete fraction must be between 0 and 1")
	}
	return nil
}

// BlockReason explains why a full-snapshot reconciliation must not execute.
type BlockReason string

const (
	BlockNone               BlockReason = ""
	BlockIncompleteLocal    BlockReason = "incomplete-local-scan"
	BlockIncompleteRemote   BlockReason = "incomplete-remote-scan"
	BlockRootUnhealthy      BlockReason = "root-unhealthy"
	BlockNamespaceUnsafe    BlockReason = "unsafe-local-namespace"
	BlockDeletePolicyUnset  BlockReason = "delete-policy-unset"
	BlockMassDeleteCount    BlockReason = "mass-delete-count"
	BlockMassDeleteFraction BlockReason = "mass-delete-fraction"
)

func evaluateDeleteGate(policy DeletePolicy, baselineCount, proposedDeletes int) (BlockReason, error) {
	if err := policy.validate(); err != nil {
		return BlockNone, err
	}
	if proposedDeletes == 0 || policy.MassDeleteApproved {
		return BlockNone, nil
	}
	if policy.MaxCount == 0 && policy.MaxFraction == 0 {
		return BlockDeletePolicyUnset, nil
	}
	if policy.MaxCount > 0 && proposedDeletes > policy.MaxCount {
		return BlockMassDeleteCount, nil
	}
	if policy.MaxFraction > 0 {
		if baselineCount <= 0 {
			return BlockMassDeleteFraction, nil
		}
		fraction := float64(proposedDeletes) / float64(baselineCount)
		if fraction > policy.MaxFraction {
			return BlockMassDeleteFraction, nil
		}
	}
	return BlockNone, nil
}
