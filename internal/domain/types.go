package domain

import (
	"fmt"
	"path"
	"strings"
)

// EntryKind is the logical kind synchronized by v1.
type EntryKind string

const (
	KindFile EntryKind = "file"
	KindDir  EntryKind = "dir"
)

// LocalFingerprint is the local state used by reconciliation and local-side
// compare-and-swap checks. Directory mtimes are deliberately not part of
// directory equality; child entries represent directory contents.
type LocalFingerprint struct {
	Present bool
	Kind    EntryKind
	Size    int64
	MtimeNS int64
}

// RemoteFingerprint is the remote state observed from PKU Disk. File revision
// is the authoritative version token. Directory identity is represented by ID.
type RemoteFingerprint struct {
	Present bool
	Kind    EntryKind
	ID      string
	Rev     string
	Size    int64
	MtimeUS int64
}

// Baseline is the last committed synchronized state for one relative path.
type Baseline struct {
	SyncRootID int64
	RelPath    string
	Local      LocalFingerprint
	Remote     RemoteFingerprint
}

// ContentEvidence records an explicit byte-content comparison. Unknown means
// no proof of equality exists; size/mtime alone never upgrades it to Equal.
type ContentEvidence uint8

const (
	ContentUnknown ContentEvidence = iota
	ContentEqual
	ContentDifferent
)

// RemoteExpectation is the precondition a future executor must carry to the
// rclone-pkudisk backend for a local-to-remote mutation.
type RemoteExpectation struct {
	Absent bool
	ID     string
	Rev    string
}

// DecisionKind is a semantic reconciliation result. The planner never performs
// I/O; Phase C will translate these decisions into executor operations.
type DecisionKind string

const (
	DecisionNoop           DecisionKind = "noop"
	DecisionCommitBaseline DecisionKind = "commit-baseline"
	DecisionEnsureRemote   DecisionKind = "ensure-remote"
	DecisionEnsureLocal    DecisionKind = "ensure-local"
	DecisionDeleteRemote   DecisionKind = "delete-remote"
	DecisionDeleteLocal    DecisionKind = "delete-local"
	DecisionDropBaseline   DecisionKind = "drop-baseline"
	DecisionCompareContent DecisionKind = "compare-content"
	DecisionConflict       DecisionKind = "conflict"
)

// ConflictKind classifies conflicts without choosing an executor choreography.
type ConflictKind string

const (
	ConflictNone                  ConflictKind = ""
	ConflictBothModified          ConflictKind = "both-modified"
	ConflictLocalDeleteRemoteEdit ConflictKind = "local-delete-remote-edit"
	ConflictRemoteDeleteLocalEdit ConflictKind = "remote-delete-local-edit"
	ConflictSimultaneousCreate    ConflictKind = "simultaneous-create"
	ConflictKindMismatch          ConflictKind = "kind-mismatch"
)

// Decision is the deterministic output for one path.
type Decision struct {
	RelPath        string
	Kind           DecisionKind
	EntryKind      EntryKind
	Conflict       ConflictKind
	Reason         string
	ExpectedLocal  LocalFingerprint
	ExpectedRemote RemoteExpectation
}

// ValidateRelPath enforces the canonical internal path representation. Paths
// are slash-separated, relative, and may not escape the sync root.
func ValidateRelPath(rel string) error {
	if rel == "" {
		return fmt.Errorf("relative path must not be empty")
	}
	if strings.ContainsRune(rel, '\\') {
		return fmt.Errorf("relative path %q must use '/' separators", rel)
	}
	clean := path.Clean(rel)
	if clean != rel || clean == "." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
		return fmt.Errorf("relative path %q is not canonical", rel)
	}
	return nil
}

func (f LocalFingerprint) Validate() error {
	if !f.Present {
		if f.Kind != "" || f.Size != 0 || f.MtimeNS != 0 {
			return fmt.Errorf("absent local fingerprint must not carry metadata")
		}
		return nil
	}
	if f.Kind != KindFile && f.Kind != KindDir {
		return fmt.Errorf("invalid local entry kind %q", f.Kind)
	}
	if f.Size < 0 {
		return fmt.Errorf("local size must be non-negative")
	}
	if f.Kind == KindDir && f.Size != 0 {
		return fmt.Errorf("directory local fingerprint must use size 0")
	}
	return nil
}

func (f RemoteFingerprint) Validate() error {
	if !f.Present {
		if f.Kind != "" || f.ID != "" || f.Rev != "" || f.Size != 0 || f.MtimeUS != 0 {
			return fmt.Errorf("absent remote fingerprint must not carry metadata")
		}
		return nil
	}
	if f.Kind != KindFile && f.Kind != KindDir {
		return fmt.Errorf("invalid remote entry kind %q", f.Kind)
	}
	if strings.TrimSpace(f.ID) == "" {
		return fmt.Errorf("present remote entry requires an ID")
	}
	if f.Size < 0 {
		return fmt.Errorf("remote size must be non-negative")
	}
	if f.Kind == KindFile && strings.TrimSpace(f.Rev) == "" {
		return fmt.Errorf("present remote file requires a revision")
	}
	if f.Kind == KindDir {
		if f.Rev != "" {
			return fmt.Errorf("directory remote fingerprint must not carry a revision")
		}
		if f.Size != 0 {
			return fmt.Errorf("directory remote fingerprint must use size 0")
		}
	}
	return nil
}

func (b Baseline) Validate() error {
	if b.SyncRootID <= 0 {
		return fmt.Errorf("sync root ID must be positive")
	}
	if err := ValidateRelPath(b.RelPath); err != nil {
		return err
	}
	if err := b.Local.Validate(); err != nil {
		return fmt.Errorf("baseline local: %w", err)
	}
	if err := b.Remote.Validate(); err != nil {
		return fmt.Errorf("baseline remote: %w", err)
	}
	if b.Local.Present && b.Remote.Present && b.Local.Kind != b.Remote.Kind {
		return fmt.Errorf("committed baseline has mismatched local/remote kinds")
	}
	return nil
}

// LocalEquivalent reports whether current local state is unchanged from the
// committed local baseline. Directory contents are compared through children.
func LocalEquivalent(a, b LocalFingerprint) bool {
	if a.Present != b.Present {
		return false
	}
	if !a.Present {
		return true
	}
	if a.Kind != b.Kind {
		return false
	}
	if a.Kind == KindDir {
		return true
	}
	return a.Size == b.Size && a.MtimeNS == b.MtimeNS
}

// RemoteEquivalent reports whether current remote state is unchanged from the
// committed remote baseline. File revision is the strongest version signal.
func RemoteEquivalent(a, b RemoteFingerprint) bool {
	if a.Present != b.Present {
		return false
	}
	if !a.Present {
		return true
	}
	if a.Kind != b.Kind || a.ID != b.ID {
		return false
	}
	if a.Kind == KindDir {
		return true
	}
	return a.Rev == b.Rev && a.Size == b.Size
}
