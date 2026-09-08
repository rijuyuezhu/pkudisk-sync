package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

// ListFollowedPhysicalClaims returns the durable physical ownership claims for
// symlink targets followed by one configured root.
func (s *Store) ListFollowedPhysicalClaims(ctx context.Context, rootID int64) (map[string]domain.FollowedPhysicalClaim, error) {
	if rootID <= 0 {
		return nil, fmt.Errorf("sync root ID must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT rel_path, kind, physical_identity, physical_target_path
FROM followed_physical_claims
WHERE sync_root_id = ?
ORDER BY rel_path`, rootID)
	if err != nil {
		return nil, fmt.Errorf("list followed physical claims: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]domain.FollowedPhysicalClaim)
	for rows.Next() {
		var relPath, kind, identity, targetPath string
		if err := rows.Scan(&relPath, &kind, &identity, &targetPath); err != nil {
			return nil, fmt.Errorf("scan followed physical claim: %w", err)
		}
		claim := domain.FollowedPhysicalClaim{Kind: domain.EntryKind(kind), Identity: identity, TargetPath: targetPath}
		if err := validateFollowedPhysicalClaim(relPath, claim, false); err != nil {
			return nil, fmt.Errorf("invalid persisted followed physical claim: %w", err)
		}
		out[relPath] = claim
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate followed physical claims: %w", err)
	}
	return out, nil
}

// ReserveFollowedPhysicalClaims atomically establishes global ownership for
// every currently observed followed symlink target before the caller may plan
// or execute external mutations. New claims are allowed only while the root is
// still in initial pairing. A physical identity already claimed by any other
// root fails closed.
//
// The boolean result is false for an authority conflict that should block the
// root without treating SQLite as unhealthy. detail is safe to show to users.
func (s *Store) ReserveFollowedPhysicalClaims(ctx context.Context, rootID int64, observed map[string]domain.FollowedPhysicalClaim) (ok bool, detail string, err error) {
	if rootID <= 0 {
		return false, "", fmt.Errorf("sync root ID must be positive")
	}
	paths, err := validateFollowedPhysicalClaims(observed, true)
	if err != nil {
		return false, "", err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", fmt.Errorf("begin followed physical claim reservation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var initialized int
	if err := tx.QueryRowContext(ctx, `SELECT initialized FROM sync_roots WHERE id = ?`, rootID).Scan(&initialized); err != nil {
		return false, "", fmt.Errorf("read sync root initialization state: %w", err)
	}
	existing, err := listFollowedPhysicalClaimsTx(ctx, tx, rootID)
	if err != nil {
		return false, "", err
	}

	for _, relPath := range paths {
		claim := observed[relPath]
		prior, exists := existing[relPath]
		if exists {
			if prior.Kind != claim.Kind {
				return false, fmt.Sprintf("followed path %q changed kind from %q to %q; re-pair the root before accepting the replacement", relPath, prior.Kind, claim.Kind), nil
			}
			if prior.Kind == domain.KindDir && prior.Identity != claim.Identity {
				return false, fmt.Sprintf("followed directory %q changed physical identity from %q to %q; re-pair the root before accepting the replacement", relPath, prior.Identity, claim.Identity), nil
			}
			if prior.TargetPath != "" && prior.TargetPath != claim.TargetPath {
				return false, fmt.Sprintf("followed %s %q changed physical target path from %q to %q; re-pair the root before accepting the replacement", claim.Kind, relPath, prior.TargetPath, claim.TargetPath), nil
			}
		} else if initialized != 0 {
			return false, fmt.Sprintf("followed %s %q has no durable physical ownership claim; re-pair the root before accepting this boundary", claim.Kind, relPath), nil
		}

		var ownerRootID int64
		var ownerPath, ownerKind string
		err := tx.QueryRowContext(ctx, `
SELECT sync_root_id, rel_path, kind
FROM followed_physical_claims
WHERE physical_identity = ?
  AND NOT (sync_root_id = ? AND rel_path = ?)
ORDER BY sync_root_id, rel_path
LIMIT 1`, claim.Identity, rootID, relPath).Scan(&ownerRootID, &ownerPath, &ownerKind)
		switch {
		case err == nil:
			return false, fmt.Sprintf("followed %s %q resolves to physical identity %q already owned by sync root %d path %q (%s)", claim.Kind, relPath, claim.Identity, ownerRootID, ownerPath, ownerKind), nil
		case err != sql.ErrNoRows:
			return false, "", fmt.Errorf("check global followed physical ownership: %w", err)
		}

		err = tx.QueryRowContext(ctx, `
SELECT sync_root_id, rel_path, kind
FROM followed_physical_claims
WHERE physical_target_path = ?
  AND physical_target_path <> ''
  AND NOT (sync_root_id = ? AND rel_path = ?)
ORDER BY sync_root_id, rel_path
LIMIT 1`, claim.TargetPath, rootID, relPath).Scan(&ownerRootID, &ownerPath, &ownerKind)
		switch {
		case err == nil:
			return false, fmt.Sprintf("followed %s %q resolves to physical target %q already owned by sync root %d path %q (%s)", claim.Kind, relPath, claim.TargetPath, ownerRootID, ownerPath, ownerKind), nil
		case err != sql.ErrNoRows:
			return false, "", fmt.Errorf("check global followed target ownership: %w", err)
		}

		if !exists {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO followed_physical_claims(sync_root_id, rel_path, kind, physical_identity, physical_target_path)
VALUES(?, ?, ?, ?, ?)`, rootID, relPath, string(claim.Kind), claim.Identity, claim.TargetPath); err != nil {
				return false, "", fmt.Errorf("reserve followed physical claim %q: %w", relPath, err)
			}
		} else if prior != claim {
			// Directory identity continuity is immutable. A followed file may be
			// atomically replaced by an editor while retaining the same canonical
			// target pathname, so its current physical identity may advance only
			// after the global identity+target ownership checks above succeed. A
			// migrated v4 directory may also backfill its target pathname here.
			if _, err := tx.ExecContext(ctx, `
UPDATE followed_physical_claims
SET physical_identity = ?, physical_target_path = ?
WHERE sync_root_id = ? AND rel_path = ?`, claim.Identity, claim.TargetPath, rootID, relPath); err != nil {
				return false, "", fmt.Errorf("refresh followed physical claim %q: %w", relPath, err)
			}
		}
		existing[relPath] = claim
	}
	if err := tx.Commit(); err != nil {
		return false, "", fmt.Errorf("commit followed physical claim reservation: %w", err)
	}
	return true, "", nil
}

// InitializeSyncRoot closes initial pairing only when the complete observed
// followed-claim set exactly matches the claims already reserved durably. This
// prevents an unavailable boundary from disappearing from the authority model
// between its first mutation and initialization commit.
func (s *Store) InitializeSyncRoot(ctx context.Context, rootID int64, observed map[string]domain.FollowedPhysicalClaim) error {
	if rootID <= 0 {
		return fmt.Errorf("sync root ID must be positive")
	}
	if _, err := validateFollowedPhysicalClaims(observed, true); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sync root initialization: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var initialized int
	if err := tx.QueryRowContext(ctx, `SELECT initialized FROM sync_roots WHERE id = ?`, rootID).Scan(&initialized); err != nil {
		return fmt.Errorf("read sync root initialization state: %w", err)
	}
	if initialized != 0 {
		return fmt.Errorf("sync root %d is already initialized", rootID)
	}
	reserved, err := listFollowedPhysicalClaimsTx(ctx, tx, rootID)
	if err != nil {
		return err
	}
	if len(reserved) != len(observed) {
		return fmt.Errorf("sync root %d followed physical claim set changed during initial pairing; re-pair before initialization", rootID)
	}
	for relPath, claim := range observed {
		if reserved[relPath] != claim {
			return fmt.Errorf("sync root %d followed physical claim %q changed during initial pairing; re-pair before initialization", rootID, relPath)
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE sync_roots SET initialized = 1 WHERE id = ? AND initialized = 0`, rootID)
	if err != nil {
		return fmt.Errorf("mark sync root initialized: %w", err)
	}
	if err := requireOneRow(result, "sync root"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sync root initialization: %w", err)
	}
	return nil
}

func listFollowedPhysicalClaimsTx(ctx context.Context, tx *sql.Tx, rootID int64) (map[string]domain.FollowedPhysicalClaim, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT rel_path, kind, physical_identity, physical_target_path
FROM followed_physical_claims
WHERE sync_root_id = ?
ORDER BY rel_path`, rootID)
	if err != nil {
		return nil, fmt.Errorf("list followed physical claims: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]domain.FollowedPhysicalClaim)
	for rows.Next() {
		var relPath, kind, identity, targetPath string
		if err := rows.Scan(&relPath, &kind, &identity, &targetPath); err != nil {
			return nil, fmt.Errorf("scan followed physical claim: %w", err)
		}
		claim := domain.FollowedPhysicalClaim{Kind: domain.EntryKind(kind), Identity: identity, TargetPath: targetPath}
		if err := validateFollowedPhysicalClaim(relPath, claim, false); err != nil {
			return nil, fmt.Errorf("invalid persisted followed physical claim: %w", err)
		}
		out[relPath] = claim
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate followed physical claims: %w", err)
	}
	return out, nil
}

func validateFollowedPhysicalClaims(claims map[string]domain.FollowedPhysicalClaim, requireTargetPath bool) ([]string, error) {
	paths := make([]string, 0, len(claims))
	identities := make(map[string]string, len(claims))
	targets := make(map[string]string, len(claims))
	for relPath, claim := range claims {
		if err := validateFollowedPhysicalClaim(relPath, claim, requireTargetPath); err != nil {
			return nil, err
		}
		if prior, exists := identities[claim.Identity]; exists && prior != relPath {
			return nil, fmt.Errorf("followed paths %q and %q claim the same physical identity %q", prior, relPath, claim.Identity)
		}
		identities[claim.Identity] = relPath
		if claim.TargetPath != "" {
			if prior, exists := targets[claim.TargetPath]; exists && prior != relPath {
				return nil, fmt.Errorf("followed paths %q and %q resolve to the same physical target path %q", prior, relPath, claim.TargetPath)
			}
			targets[claim.TargetPath] = relPath
		}
		paths = append(paths, relPath)
	}
	sort.Strings(paths)
	return paths, nil
}

func validateFollowedPhysicalClaim(relPath string, claim domain.FollowedPhysicalClaim, requireTargetPath bool) error {
	if err := domain.ValidateRelPath(relPath); err != nil {
		return fmt.Errorf("followed physical claim path: %w", err)
	}
	if claim.Kind != domain.KindFile && claim.Kind != domain.KindDir {
		return fmt.Errorf("followed physical claim %q has invalid kind %q", relPath, claim.Kind)
	}
	if claim.Identity == "" {
		return fmt.Errorf("followed physical claim %q has empty physical identity", relPath)
	}
	if claim.TargetPath == "" {
		if requireTargetPath {
			return fmt.Errorf("followed physical claim %q has empty physical target path", relPath)
		}
		if claim.Kind != domain.KindDir {
			return fmt.Errorf("followed physical claim %q has empty physical target path for non-directory state", relPath)
		}
		return nil
	}
	if !filepath.IsAbs(claim.TargetPath) || filepath.Clean(claim.TargetPath) != claim.TargetPath {
		return fmt.Errorf("followed physical claim %q has non-canonical absolute target path %q", relPath, claim.TargetPath)
	}
	return nil
}
