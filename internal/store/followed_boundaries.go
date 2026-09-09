package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

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

func checkSyncRootAgainstFollowedClaims(ctx context.Context, q rowsQuerier, candidate domain.SyncRoot) error {
	rows, err := q.QueryContext(ctx, `
SELECT sync_root_id, rel_path, kind, physical_target_path
FROM followed_physical_claims
ORDER BY sync_root_id, rel_path`)
	if err != nil {
		return fmt.Errorf("list followed physical claims for sync root ownership: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ownerRootID int64
		var relPath, kind, targetPath string
		if err := rows.Scan(&ownerRootID, &relPath, &kind, &targetPath); err != nil {
			return fmt.Errorf("scan followed physical claim for sync root ownership: %w", err)
		}
		ownerKind := domain.EntryKind(kind)
		if ownerKind != domain.KindFile && ownerKind != domain.KindDir {
			return fmt.Errorf("followed physical claim for sync root %d path %q has invalid kind %q", ownerRootID, relPath, kind)
		}
		if targetPath == "" {
			return fmt.Errorf("cannot prove local ownership for new sync root %q while sync root %d path %q has a legacy followed directory with unknown physical target; run or re-pair that root first", candidate.LocalRoot, ownerRootID, relPath)
		}
		if physicalOwnershipPathsOverlap(candidate.LocalRoot, domain.KindDir, targetPath, ownerKind) {
			return fmt.Errorf("local sync root %q overlaps followed %s target %q owned by sync root %d path %q", candidate.LocalRoot, ownerKind, targetPath, ownerRootID, relPath)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate followed physical claims for sync root ownership: %w", err)
	}
	return nil
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
	// Acquire SQLite's single-writer authority before any ownership read. This
	// makes hierarchical read-check-write reservation serial across Store
	// instances/processes rather than relying on an in-process mutex.
	result, err := tx.ExecContext(ctx, `UPDATE sync_roots SET initialized = initialized WHERE id = ?`, rootID)
	if err != nil {
		return false, "", fmt.Errorf("reserve followed physical claim writer authority: %w", err)
	}
	if err := requireOneRow(result, "sync root"); err != nil {
		return false, "", err
	}

	var initialized int
	if err := tx.QueryRowContext(ctx, `SELECT initialized FROM sync_roots WHERE id = ?`, rootID).Scan(&initialized); err != nil {
		return false, "", fmt.Errorf("read sync root initialization state: %w", err)
	}
	existing, err := listFollowedPhysicalClaimsTx(ctx, tx, rootID)
	if err != nil {
		return false, "", err
	}

	// v4->v5 migration knew directory identities but not canonical target
	// paths. Recover all such paths together from this complete scan before
	// subtree arbitration; an unavailable legacy boundary keeps authority
	// ambiguous and therefore blocks rather than allowing a partial backfill.
	for relPath, prior := range existing {
		if prior.TargetPath != "" {
			continue
		}
		current, exists := observed[relPath]
		if !exists {
			return false, fmt.Sprintf("legacy followed directory %q has unknown physical target and is unavailable; re-pair or restore it before ownership can be proven", relPath), nil
		}
		if prior.Kind != current.Kind || prior.Identity != current.Identity {
			return false, fmt.Sprintf("legacy followed directory %q changed before its physical target path could be recovered; re-pair the root", relPath), nil
		}
		if conflict, err := followedOwnershipConflictTx(ctx, tx, rootID, relPath, current, true); err != nil {
			return false, "", err
		} else if conflict != "" {
			return false, conflict, nil
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE followed_physical_claims
SET physical_target_path = ?
WHERE sync_root_id = ? AND rel_path = ? AND physical_target_path = ''`, current.TargetPath, rootID, relPath); err != nil {
			return false, "", fmt.Errorf("backfill followed physical target path %q: %w", relPath, err)
		}
		existing[relPath] = current
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

		if conflict, err := followedOwnershipConflictTx(ctx, tx, rootID, relPath, claim, false); err != nil {
			return false, "", err
		} else if conflict != "" {
			return false, conflict, nil
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

// AuthorizeAndPinLocalMutation binds a planned local operation to the physical
// target resolved immediately before execution. It never learns a new followed
// boundary: every traversed symlink must already belong to this root's durable
// scan-time ownership set. Validation, safe file-identity refresh, and the
// physical target path+anchor journal update are committed in one SQLite transaction.
func (s *Store) AuthorizeAndPinLocalMutation(ctx context.Context, operationID int64, target domain.LocalMutationTarget) (ok bool, detail string, err error) {
	if operationID <= 0 {
		return false, "", fmt.Errorf("operation ID must be positive")
	}
	if target.Path == "" || !filepath.IsAbs(target.Path) || filepath.Clean(target.Path) != target.Path {
		return false, "", fmt.Errorf("local mutation target %q must be canonical and absolute", target.Path)
	}
	if target.AnchorIdentity == "" {
		return false, "", fmt.Errorf("local mutation target %q has no physical anchor identity", target.Path)
	}
	authority := target.Authority
	if authority == "" {
		if len(target.FollowedClaims) > 0 {
			authority = domain.LocalMutationFollowPhysical
		} else {
			authority = domain.LocalMutationLexical
		}
	}
	switch authority {
	case domain.LocalMutationLexical, domain.LocalMutationFollowPhysical, domain.LocalMutationCopyPhysical:
	default:
		return false, "", fmt.Errorf("invalid local mutation authority %q", authority)
	}
	if authority == domain.LocalMutationFollowPhysical {
		if err := validateLocalMutationClaims(target.FollowedClaims); err != nil {
			return false, "", err
		}
	} else if len(target.FollowedClaims) != 0 {
		return false, "", fmt.Errorf("local mutation authority %q must not carry followed physical claims", authority)
	}
	if target.SymlinkTarget != "" && authority != domain.LocalMutationLexical && authority != domain.LocalMutationCopyPhysical {
		return false, "", fmt.Errorf("local symlink target metadata requires lexical or copy-physical mutation authority")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", fmt.Errorf("begin local mutation authority transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Mutation-time claim refresh must serialize with root creation and scan-time
	// claim reservation before it reads global ownership.
	result, err := tx.ExecContext(ctx, `UPDATE operations SET updated_at_ns = updated_at_ns WHERE id = ?`, operationID)
	if err != nil {
		return false, "", fmt.Errorf("reserve local mutation writer authority: %w", err)
	}
	if err := requireOneRow(result, "operation"); err != nil {
		return false, "", err
	}

	var rootID int64
	var operationKind, srcPath, phase, pinned string
	var rootSymlinkMode string
	var attempts int
	if err := tx.QueryRowContext(ctx, `
SELECT operations.sync_root_id, operations.kind, operations.src_path, operations.phase,
       operations.attempts, operations.local_target_path, sync_roots.symlink_mode
FROM operations
JOIN sync_roots ON sync_roots.id = operations.sync_root_id
WHERE operations.id = ?`, operationID).Scan(&rootID, &operationKind, &srcPath, &phase, &attempts, &pinned, &rootSymlinkMode); err != nil {
		if err == sql.ErrNoRows {
			return false, "", fmt.Errorf("operation %d not found", operationID)
		}
		return false, "", fmt.Errorf("read operation for local mutation authority: %w", err)
	}
	if operationKind != string(domain.OperationEnsureLocal) && operationKind != string(domain.OperationDeleteLocal) {
		return false, "", fmt.Errorf("operation %d is not a local mutation", operationID)
	}
	if phase != string(domain.OperationPlanned) || attempts != 0 || pinned != "" {
		return false, "", fmt.Errorf("operation %d is no longer an unpinned planned mutation", operationID)
	}
	if err := domain.ValidateRelPath(srcPath); err != nil {
		return false, "", fmt.Errorf("operation %d source path: %w", operationID, err)
	}
	if authority == domain.LocalMutationCopyPhysical && rootSymlinkMode != string(domain.SymlinkCopy) {
		return false, "", fmt.Errorf("copy-physical mutation authority requires root symlink mode copy")
	}
	if target.SymlinkTarget != "" && rootSymlinkMode != string(domain.SymlinkCopy) {
		return false, "", fmt.Errorf("copy lexical symlink metadata requires root symlink mode copy")
	}

	if authority == domain.LocalMutationFollowPhysical {
		durable, err := listFollowedPhysicalClaimsTx(ctx, tx, rootID)
		if err != nil {
			return false, "", err
		}

		for relPath, current := range target.FollowedClaims {
		if !followedClaimAppliesToMutation(relPath, current.Kind, srcPath) {
			return false, "", fmt.Errorf("followed claim %q does not belong to mutation path %q", relPath, srcPath)
		}
		prior, exists := durable[relPath]
		if !exists {
			return false, fmt.Sprintf("followed %s %q appeared after the authoritative scan; rescan before mutating %q", current.Kind, relPath, srcPath), nil
		}
		if prior.Kind != current.Kind {
			return false, fmt.Sprintf("followed path %q changed kind from %q to %q before local mutation pin", relPath, prior.Kind, current.Kind), nil
		}
		if prior.TargetPath != "" && prior.TargetPath != current.TargetPath {
			return false, fmt.Sprintf("followed %s %q changed physical target from %q to %q before local mutation pin", current.Kind, relPath, prior.TargetPath, current.TargetPath), nil
		}
		if current.Identity == "" {
			if current.Kind != domain.KindFile {
				return false, fmt.Sprintf("followed directory %q is unavailable at local mutation pin; directory identity continuity cannot be proven", relPath), nil
			}
			if prior.TargetPath == "" || prior.TargetPath != current.TargetPath {
				return false, fmt.Sprintf("dangling followed file %q has no matching durable target authority", relPath), nil
			}
		}
		if current.Kind == domain.KindDir && prior.Identity != current.Identity {
			return false, fmt.Sprintf("followed directory %q changed physical identity from %q to %q before local mutation pin", relPath, prior.Identity, current.Identity), nil
		}
		if conflict, err := followedOwnershipConflictTx(ctx, tx, rootID, relPath, current, false); err != nil {
			return false, "", err
		} else if conflict != "" {
			return false, conflict, nil
		}
		if current.Identity != "" && prior != current {
			if _, err := tx.ExecContext(ctx, `
UPDATE followed_physical_claims
SET physical_identity = ?, physical_target_path = ?
WHERE sync_root_id = ? AND rel_path = ?`, current.Identity, current.TargetPath, rootID, relPath); err != nil {
				return false, "", fmt.Errorf("refresh followed physical authority %q during local mutation pin: %w", relPath, err)
			}
		}
		}

		for relPath, prior := range durable {
		if !followedClaimAppliesToMutation(relPath, prior.Kind, srcPath) {
			continue
		}
		if _, exists := target.FollowedClaims[relPath]; !exists {
			return false, fmt.Sprintf("durable followed %s %q is no longer traversed while pinning local mutation %q", prior.Kind, relPath, srcPath), nil
		}
		}

		if expected, hasBoundary, err := mutationTargetFromDeepestClaim(srcPath, target.FollowedClaims); err != nil {
		return false, "", err
	} else if hasBoundary {
		if filepath.Clean(expected) != target.Path {
			return false, fmt.Sprintf("resolved local mutation target %q is not derived from durable followed authority %q", target.Path, expected), nil
		}
		}
	}

	result, err = tx.ExecContext(ctx, `
UPDATE operations
SET local_target_path = ?, local_target_identity = ?, local_target_authority = ?,
    local_symlink_target = ?, updated_at_ns = ?
WHERE id = ? AND phase = ? AND attempts = 0 AND local_target_path = ''
  AND local_target_identity = '' AND local_target_authority = ''`,
		target.Path, target.AnchorIdentity, string(authority), target.SymlinkTarget,
		s.now().UnixNano(), operationID, string(domain.OperationPlanned))
	if err != nil {
		return false, "", fmt.Errorf("pin authorized local mutation target: %w", err)
	}
	if err := requireOneRow(result, "planned operation"); err != nil {
		return false, "", err
	}
	if err := tx.Commit(); err != nil {
		return false, "", fmt.Errorf("commit local mutation authority: %w", err)
	}
	return true, "", nil
}

func followedClaimAppliesToMutation(claimPath string, kind domain.EntryKind, mutationPath string) bool {
	if claimPath == mutationPath {
		return true
	}
	return kind == domain.KindDir && strings.HasPrefix(mutationPath, claimPath+"/")
}

func validateLocalMutationClaims(claims map[string]domain.FollowedPhysicalClaim) error {
	paths := make([]string, 0, len(claims))
	targets := make(map[string]string, len(claims))
	identities := make(map[string]string, len(claims))
	for relPath, claim := range claims {
		if err := domain.ValidateRelPath(relPath); err != nil {
			return fmt.Errorf("local mutation followed claim path: %w", err)
		}
		if claim.Kind != domain.KindFile && claim.Kind != domain.KindDir {
			return fmt.Errorf("local mutation followed claim %q has invalid kind %q", relPath, claim.Kind)
		}
		if claim.TargetPath == "" || !filepath.IsAbs(claim.TargetPath) || filepath.Clean(claim.TargetPath) != claim.TargetPath {
			return fmt.Errorf("local mutation followed claim %q has invalid physical target path %q", relPath, claim.TargetPath)
		}
		if prior, exists := targets[claim.TargetPath]; exists && prior != relPath {
			return fmt.Errorf("local mutation followed paths %q and %q resolve to the same physical target %q", prior, relPath, claim.TargetPath)
		}
		targets[claim.TargetPath] = relPath
		if claim.Identity != "" {
			if prior, exists := identities[claim.Identity]; exists && prior != relPath {
				return fmt.Errorf("local mutation followed paths %q and %q claim the same physical identity %q", prior, relPath, claim.Identity)
			}
			identities[claim.Identity] = relPath
		}
		paths = append(paths, relPath)
	}
	sort.Strings(paths)
	for i := 0; i < len(paths); i++ {
		leftPath := paths[i]
		left := claims[leftPath]
		for j := i + 1; j < len(paths); j++ {
			rightPath := paths[j]
			right := claims[rightPath]
			if physicalOwnershipPathsOverlap(left.TargetPath, left.Kind, right.TargetPath, right.Kind) {
				return fmt.Errorf("local mutation followed paths %q and %q claim overlapping physical target subtrees %q and %q", leftPath, rightPath, left.TargetPath, right.TargetPath)
			}
		}
	}
	return nil
}

func mutationTargetFromDeepestClaim(mutationPath string, claims map[string]domain.FollowedPhysicalClaim) (string, bool, error) {
	deepest := ""
	for relPath, claim := range claims {
		if !followedClaimAppliesToMutation(relPath, claim.Kind, mutationPath) {
			return "", false, fmt.Errorf("followed claim %q does not apply to mutation path %q", relPath, mutationPath)
		}
		if len(relPath) > len(deepest) {
			deepest = relPath
		}
	}
	if deepest == "" {
		return "", false, nil
	}
	claim := claims[deepest]
	if deepest == mutationPath {
		return claim.TargetPath, true, nil
	}
	if claim.Kind != domain.KindDir {
		return "", false, fmt.Errorf("followed file %q cannot authorize descendant mutation %q", deepest, mutationPath)
	}
	suffix := strings.TrimPrefix(mutationPath, deepest+"/")
	return filepath.Join(claim.TargetPath, filepath.FromSlash(suffix)), true, nil
}

func followedOwnershipConflictTx(ctx context.Context, tx *sql.Tx, rootID int64, relPath string, claim domain.FollowedPhysicalClaim, allowSameRootLegacyUnknown bool) (string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT sync_root_id, rel_path, kind, physical_identity, physical_target_path
FROM followed_physical_claims
WHERE NOT (sync_root_id = ? AND rel_path = ?)
ORDER BY sync_root_id, rel_path`, rootID, relPath)
	if err != nil {
		return "", fmt.Errorf("list global followed physical ownership: %w", err)
	}
	for rows.Next() {
		var ownerRootID int64
		var ownerPath, ownerKindRaw, ownerIdentity, ownerTarget string
		if err := rows.Scan(&ownerRootID, &ownerPath, &ownerKindRaw, &ownerIdentity, &ownerTarget); err != nil {
			_ = rows.Close()
			return "", fmt.Errorf("scan global followed physical ownership: %w", err)
		}
		ownerKind := domain.EntryKind(ownerKindRaw)
		if ownerKind != domain.KindFile && ownerKind != domain.KindDir {
			_ = rows.Close()
			return "", fmt.Errorf("global followed owner root %d path %q has invalid kind %q", ownerRootID, ownerPath, ownerKindRaw)
		}
		if claim.Identity != "" && ownerIdentity == claim.Identity {
			_ = rows.Close()
			return fmt.Sprintf("followed %s %q resolves to physical identity %q already owned by sync root %d path %q (%s)", claim.Kind, relPath, claim.Identity, ownerRootID, ownerPath, ownerKind), nil
		}
		if ownerTarget == "" {
			if allowSameRootLegacyUnknown && ownerRootID == rootID {
				continue
			}
			_ = rows.Close()
			return fmt.Sprintf("followed %s %q cannot prove subtree ownership because sync root %d path %q (%s) has an unknown legacy physical target", claim.Kind, relPath, ownerRootID, ownerPath, ownerKind), nil
		}
		if physicalOwnershipPathsOverlap(ownerTarget, ownerKind, claim.TargetPath, claim.Kind) {
			_ = rows.Close()
			return fmt.Sprintf("followed %s %q resolves to physical target subtree %q overlapping target %q already owned by sync root %d path %q (%s)", claim.Kind, relPath, claim.TargetPath, ownerTarget, ownerRootID, ownerPath, ownerKind), nil
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return "", fmt.Errorf("iterate global followed physical ownership: %w", err)
	}
	if err := rows.Close(); err != nil {
		return "", fmt.Errorf("close global followed physical ownership rows: %w", err)
	}

	rootRows, err := tx.QueryContext(ctx, `SELECT id, local_root FROM sync_roots WHERE id <> ? ORDER BY id`, rootID)
	if err != nil {
		return "", fmt.Errorf("list configured roots for followed ownership: %w", err)
	}
	defer func() { _ = rootRows.Close() }()
	for rootRows.Next() {
		var peerRootID int64
		var peerLocalRoot string
		if err := rootRows.Scan(&peerRootID, &peerLocalRoot); err != nil {
			return "", fmt.Errorf("scan configured root for followed ownership: %w", err)
		}
		if physicalOwnershipPathsOverlap(peerLocalRoot, domain.KindDir, claim.TargetPath, claim.Kind) {
			return fmt.Sprintf("followed %s %q resolves to physical target subtree %q overlapping configured sync root %d at %q", claim.Kind, relPath, claim.TargetPath, peerRootID, peerLocalRoot), nil
		}
	}
	if err := rootRows.Err(); err != nil {
		return "", fmt.Errorf("iterate configured roots for followed ownership: %w", err)
	}
	return "", nil
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
	for i := 0; i < len(paths); i++ {
		leftPath := paths[i]
		left := claims[leftPath]
		for j := i + 1; j < len(paths); j++ {
			rightPath := paths[j]
			right := claims[rightPath]
			if physicalOwnershipPathsOverlap(left.TargetPath, left.Kind, right.TargetPath, right.Kind) {
				return nil, fmt.Errorf("followed paths %q and %q claim overlapping physical target subtrees %q and %q", leftPath, rightPath, left.TargetPath, right.TargetPath)
			}
		}
	}
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
