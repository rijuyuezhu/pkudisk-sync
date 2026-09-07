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
