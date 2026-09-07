package daemon

import (
	"context"
	"errors"
	"fmt"
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
	if err := rootmarker.Ensure(root.LocalRoot, root.UUID); err != nil {
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
	if err := rootmarker.Remove(root.LocalRoot, root.UUID); err != nil {
		return domain.SyncRoot{}, fmt.Errorf("remove sync root marker: %w", err)
	}
	if err := state.DeleteSyncRoot(ctx, id); err != nil {
		restoreErr := rootmarker.Ensure(root.LocalRoot, root.UUID)
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
