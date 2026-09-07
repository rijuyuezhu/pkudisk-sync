package domain

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// SyncRoot is one independently reconciled local/remote root pair.
type SyncRoot struct {
	ID                  int64
	UUID                string
	LocalRoot           string
	RemoteName          string
	RemoteRoot          string
	Enabled             bool
	Initialized         bool
	PollIntervalSeconds int64
	CreatedAt           time.Time
}

func (r SyncRoot) Validate() error {
	if strings.TrimSpace(r.UUID) == "" {
		return fmt.Errorf("sync root UUID must not be empty")
	}
	if strings.TrimSpace(r.LocalRoot) == "" {
		return fmt.Errorf("local root must not be empty")
	}
	if !filepath.IsAbs(r.LocalRoot) {
		return fmt.Errorf("local root %q must be absolute", r.LocalRoot)
	}
	if filepath.Clean(r.LocalRoot) != r.LocalRoot {
		return fmt.Errorf("local root %q must be canonical", r.LocalRoot)
	}
	if strings.TrimSpace(r.RemoteName) == "" {
		return fmt.Errorf("remote name must not be empty")
	}
	if strings.TrimSpace(r.RemoteRoot) == "" {
		return fmt.Errorf("remote root must not be empty")
	}
	if path.Clean(r.RemoteRoot) != r.RemoteRoot || strings.HasPrefix(r.RemoteRoot, "/") || r.RemoteRoot == "." || strings.HasPrefix(r.RemoteRoot, "../") {
		return fmt.Errorf("remote root %q must be a canonical relative path", r.RemoteRoot)
	}
	if r.PollIntervalSeconds < 0 {
		return fmt.Errorf("poll interval must be non-negative")
	}
	return nil
}

// OperationKind names a durable semantic external-side-effect intent.
type OperationKind string

const (
	OperationEnsureRemote OperationKind = "ensure-remote"
	OperationEnsureLocal  OperationKind = "ensure-local"
	OperationDeleteRemote OperationKind = "delete-remote"
	OperationDeleteLocal  OperationKind = "delete-local"
	OperationMoveRemote   OperationKind = "move-remote"
	OperationMoveLocal    OperationKind = "move-local"
)

// OperationPhase models crash recovery. Running explicitly means outcome
// unknown after a crash; it does not mean the operation is safe to replay.
type OperationPhase string

const (
	OperationPlanned    OperationPhase = "planned"
	OperationRunning    OperationPhase = "running"
	OperationRecovering OperationPhase = "recovering"
	OperationBlocked    OperationPhase = "blocked"
)

// Operation is the durable semantic intent persisted before an external side
// effect. Byte-transfer chunk/progress state belongs to rclone-pkudisk instead.
type Operation struct {
	ID             int64
	SyncRootID     int64
	Kind           OperationKind
	EntryKind      EntryKind
	SrcPath        string
	DstPath        string
	ExpectedLocal  LocalFingerprint
	ExpectedRemote RemoteExpectation
	Phase          OperationPhase
	Attempts       int
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (o Operation) Validate() error {
	if o.SyncRootID <= 0 {
		return fmt.Errorf("operation sync root ID must be positive")
	}
	switch o.Kind {
	case OperationEnsureRemote, OperationEnsureLocal, OperationDeleteRemote, OperationDeleteLocal, OperationMoveRemote, OperationMoveLocal:
	default:
		return fmt.Errorf("invalid operation kind %q", o.Kind)
	}
	if o.EntryKind != KindFile && o.EntryKind != KindDir {
		return fmt.Errorf("invalid operation entry kind %q", o.EntryKind)
	}
	if err := ValidateRelPath(o.SrcPath); err != nil {
		return fmt.Errorf("operation source path: %w", err)
	}
	if o.Kind == OperationMoveRemote || o.Kind == OperationMoveLocal {
		if err := ValidateRelPath(o.DstPath); err != nil {
			return fmt.Errorf("operation destination path: %w", err)
		}
	} else if o.DstPath != "" {
		return fmt.Errorf("non-move operation must not have a destination path")
	}
	if err := o.ExpectedLocal.Validate(); err != nil {
		return fmt.Errorf("operation expected local state: %w", err)
	}
	if err := o.ExpectedRemote.Validate(); err != nil {
		return fmt.Errorf("operation expected remote state: %w", err)
	}
	if !o.ExpectedRemote.Absent {
		if o.EntryKind == KindFile && strings.TrimSpace(o.ExpectedRemote.Rev) == "" {
			return fmt.Errorf("file operation with present remote expectation requires a revision")
		}
		if o.EntryKind == KindDir && o.ExpectedRemote.Rev != "" {
			return fmt.Errorf("directory operation remote expectation must not carry a revision")
		}
	}
	switch o.Phase {
	case OperationPlanned, OperationRunning, OperationRecovering, OperationBlocked:
	default:
		return fmt.Errorf("invalid operation phase %q", o.Phase)
	}
	if o.Attempts < 0 {
		return fmt.Errorf("operation attempts must be non-negative")
	}
	return nil
}

func (e RemoteExpectation) Validate() error {
	if e.Absent {
		if e.ID != "" || e.Rev != "" {
			return fmt.Errorf("remote-absent expectation must not carry ID/revision")
		}
		return nil
	}
	if strings.TrimSpace(e.ID) == "" {
		return fmt.Errorf("remote-present expectation requires an ID")
	}
	return nil
}

// Conflict is a durable user-visible record of a conflict discovered by the
// planner. It stores the observed states that caused the conflict.
type Conflict struct {
	ID         int64
	SyncRootID int64
	RelPath    string
	Kind       ConflictKind
	Local      LocalFingerprint
	Remote     RemoteFingerprint
	Resolved   bool
	CreatedAt  time.Time
	ResolvedAt time.Time
}

func (c Conflict) Validate() error {
	if c.SyncRootID <= 0 {
		return fmt.Errorf("conflict sync root ID must be positive")
	}
	if err := ValidateRelPath(c.RelPath); err != nil {
		return err
	}
	switch c.Kind {
	case ConflictBothModified, ConflictLocalDeleteRemoteEdit, ConflictRemoteDeleteLocalEdit, ConflictSimultaneousCreate, ConflictKindMismatch:
	default:
		return fmt.Errorf("invalid conflict kind %q", c.Kind)
	}
	if err := c.Local.Validate(); err != nil {
		return fmt.Errorf("conflict local state: %w", err)
	}
	if err := c.Remote.Validate(); err != nil {
		return fmt.Errorf("conflict remote state: %w", err)
	}
	return nil
}
