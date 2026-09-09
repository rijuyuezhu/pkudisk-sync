package domain

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const maxPollIntervalSeconds int64 = (1<<63 - 1) / int64(time.Second)

// AppRemoteName is the single app-owned PKU Disk authentication authority in
// v0.1. Multiple selected roots share this profile; multiple accounts/profiles
// need an explicit stable account-identity model before they can be safe.
const AppRemoteName = "pkudisk"

// SymlinkMode controls how one local sync root treats symbolic links.
type SymlinkMode string

const (
	// SymlinkFollow dereferences links into the virtual sync namespace.
	SymlinkFollow SymlinkMode = "follow"
	// SymlinkReject makes any symlink a complete-local-scan error.
	SymlinkReject SymlinkMode = "reject"
	// SymlinkIgnore excludes every symlink path and its virtual subtree.
	SymlinkIgnore SymlinkMode = "ignore"
)

// ParseSymlinkMode validates a user/config value. An empty value selects the
// default follow policy.
func ParseSymlinkMode(value string) (SymlinkMode, error) {
	mode := SymlinkMode(strings.TrimSpace(value))
	if mode == "" {
		mode = SymlinkFollow
	}
	switch mode {
	case SymlinkFollow, SymlinkReject, SymlinkIgnore:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid symlink mode %q; want follow, reject, or ignore", value)
	}
}

// SyncRoot is one independently reconciled local/remote root pair.
type SyncRoot struct {
	ID                  int64
	UUID                string
	LocalRoot           string
	RemoteName          string
	RemoteRoot          string
	Enabled             bool
	Initialized         bool
	SymlinkMode         SymlinkMode
	PollIntervalSeconds int64
	CreatedAt           time.Time
}

// EffectiveSymlinkMode returns the configured mode. The zero value means the
// product default, follow, so old call sites and migrated state remain safe.
func (r SyncRoot) EffectiveSymlinkMode() SymlinkMode {
	if r.SymlinkMode == "" {
		return SymlinkFollow
	}
	return r.SymlinkMode
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
	if r.RemoteName != AppRemoteName {
		return fmt.Errorf("remote name %q is unsupported; v0.1 uses the single app-owned remote %q", r.RemoteName, AppRemoteName)
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
	if r.PollIntervalSeconds > maxPollIntervalSeconds {
		return fmt.Errorf("poll interval exceeds time.Duration range")
	}
	if _, err := ParseSymlinkMode(string(r.SymlinkMode)); err != nil {
		return err
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
	ID              int64
	SyncRootID      int64
	Kind            OperationKind
	EntryKind       EntryKind
	SrcPath         string
	DstPath         string
	LocalTargetPath string
	// LocalTargetIdentity is the physical anchor pinned with LocalTargetPath:
	// the target object's identity when ExpectedLocal is present, otherwise
	// the containing directory identity. Empty is retained only for migrated
	// pre-v6 operations that must fail closed before automatic replay.
	LocalTargetIdentity string
	ExpectedLocal       LocalFingerprint
	ExpectedRemote      RemoteExpectation
	Phase               OperationPhase
	Attempts            int
	LastError           string
	CreatedAt           time.Time
	UpdatedAt           time.Time
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
	if o.LocalTargetPath != "" {
		if o.Kind != OperationEnsureLocal && o.Kind != OperationDeleteLocal {
			return fmt.Errorf("operation kind %q must not carry a local target path", o.Kind)
		}
		if !filepath.IsAbs(o.LocalTargetPath) || filepath.Clean(o.LocalTargetPath) != o.LocalTargetPath {
			return fmt.Errorf("operation local target path %q must be canonical and absolute", o.LocalTargetPath)
		}
	}
	if o.LocalTargetIdentity != "" {
		if o.Kind != OperationEnsureLocal && o.Kind != OperationDeleteLocal {
			return fmt.Errorf("operation kind %q must not carry a local target identity", o.Kind)
		}
		if o.LocalTargetPath == "" {
			return fmt.Errorf("operation local target identity requires a pinned local target path")
		}
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
