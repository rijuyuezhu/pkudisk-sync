package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
)

var setupRootMu sync.Mutex

// SetupRoot is the product-level pairing operation. It first acquires an
// authoritative SQLite ownership reservation, then establishes the local root
// marker, then commits the durable root row. A final commit error preserves the
// marker deliberately because the durable outcome is uncertain.
func SetupRoot(ctx context.Context, state *store.Store, root domain.SyncRoot) (domain.SyncRoot, error) {
	return setupRoot(ctx, state, root, false)
}

// SetupRootRecoveringOrphanMarker is the explicit recovery variant for a root
// add that was interrupted after writing the reserved marker but before the
// SQLite root row committed. Because a marker may fence a different state DB,
// callers must expose this as an explicit user choice rather than auto-adopting
// or replacing it.
func SetupRootRecoveringOrphanMarker(ctx context.Context, state *store.Store, root domain.SyncRoot) (domain.SyncRoot, error) {
	return setupRoot(ctx, state, root, true)
}

func setupRoot(ctx context.Context, state *store.Store, root domain.SyncRoot, recoverOrphanMarker bool) (domain.SyncRoot, error) {
	// Root pairing spans the filesystem marker and SQLite registration. The
	// product has one controlling daemon, so serialize this rare workflow to
	// prevent two concurrent setup calls from disagreeing about marker ownership.
	setupRootMu.Lock()
	defer setupRootMu.Unlock()

	if state == nil {
		return domain.SyncRoot{}, fmt.Errorf("state store must not be nil")
	}
	reservation, err := state.PrepareSyncRootCreate(ctx, root)
	if err != nil {
		return domain.SyncRoot{}, fmt.Errorf("reserve sync root ownership: %w", err)
	}
	if err := rootmarker.Ensure(root.LocalRoot, root.UUID); err != nil && recoverOrphanMarker {
		actual, readErr := rootmarker.Read(root.LocalRoot)
		if readErr != nil {
			rollbackErr := reservation.Close()
			return domain.SyncRoot{}, errors.Join(
				fmt.Errorf("read orphan sync root marker: %w", readErr),
				rollbackErr,
			)
		}
		if removeErr := rootmarker.Remove(root.LocalRoot, actual); removeErr != nil {
			rollbackErr := reservation.Close()
			return domain.SyncRoot{}, errors.Join(
				fmt.Errorf("remove orphan sync root marker: %w", removeErr),
				rollbackErr,
			)
		}
		if ensureErr := rootmarker.Ensure(root.LocalRoot, root.UUID); ensureErr != nil {
			rollbackErr := reservation.Close()
			return domain.SyncRoot{}, errors.Join(
				fmt.Errorf("replace orphan sync root marker: %w", ensureErr),
				rollbackErr,
			)
		}
	} else if err != nil {
		rollbackErr := reservation.Close()
		if rollbackErr != nil {
			return domain.SyncRoot{}, errors.Join(
				fmt.Errorf("establish sync root marker: %w", err),
				rollbackErr,
			)
		}
		return domain.SyncRoot{}, fmt.Errorf("establish sync root marker: %w", err)
	}
	stored, err := reservation.Commit(ctx)
	if err != nil {
		// Fail closed. Keep the marker so an uncertain durable outcome can never
		// turn into a configured root that silently lacks its safety identity.
		rollbackErr := reservation.Close()
		if rollbackErr != nil {
			return domain.SyncRoot{}, errors.Join(
				fmt.Errorf("register sync root after marker established: %w", err),
				rollbackErr,
			)
		}
		return domain.SyncRoot{}, fmt.Errorf("register sync root after marker established: %w", err)
	}
	return stored, nil
}

// RemoveRoot unregisters one selected directory pair without deleting any
// local or remote user content. The caller must separately exclude a running
// daemon for the duration of this workflow.
func RemoveRoot(ctx context.Context, state *store.Store, id int64) (domain.SyncRoot, error) {
	setupRootMu.Lock()
	defer setupRootMu.Unlock()

	if state == nil {
		return domain.SyncRoot{}, fmt.Errorf("state store must not be nil")
	}
	root, ok, err := state.GetSyncRoot(ctx, id)
	if err != nil {
		return domain.SyncRoot{}, err
	}
	if !ok {
		return domain.SyncRoot{}, fmt.Errorf("sync root %d not found", id)
	}
	if root.Enabled {
		return domain.SyncRoot{}, fmt.Errorf("sync root %d must be paused before removal", id)
	}
	operations, err := state.ListOperations(ctx, id)
	if err != nil {
		return domain.SyncRoot{}, err
	}
	if len(operations) != 0 {
		return domain.SyncRoot{}, fmt.Errorf("sync root %d has %d pending operations; reconcile or recover them before removal", id, len(operations))
	}
	markerMissing := false
	if err := rootmarker.Remove(root.LocalRoot, root.UUID); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			markerMissing = true
		} else {
			return domain.SyncRoot{}, fmt.Errorf("remove sync root marker: %w", err)
		}
	}
	if err := state.DeleteSyncRoot(ctx, id); err != nil {
		var restoreErr error
		if !markerMissing {
			restoreErr = rootmarker.Ensure(root.LocalRoot, root.UUID)
		}
		if restoreErr != nil {
			return domain.SyncRoot{}, errors.Join(
				fmt.Errorf("unregister sync root after marker removal: %w", err),
				fmt.Errorf("restore sync root marker: %w", restoreErr),
			)
		}
		return domain.SyncRoot{}, fmt.Errorf("unregister sync root after marker removal: %w", err)
	}
	return root, nil
}
