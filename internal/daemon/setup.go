package daemon

import (
	"context"
	"fmt"
	"sync"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
	"github.com/rijuyuezhu/pkudisk-sync/internal/rootmarker"
	"github.com/rijuyuezhu/pkudisk-sync/internal/store"
)

var setupRootMu sync.Mutex

// SetupRoot is the product-level pairing operation. It preflights deterministic
// ownership conflicts before creating the local marker, then publishes the
// durable root record. If the final durable step fails, the marker is preserved
// deliberately: that failure does not prove the database has no side effect.
func SetupRoot(ctx context.Context, state *store.Store, root domain.SyncRoot) (domain.SyncRoot, error) {
	// Root pairing spans the filesystem marker and SQLite registration. The
	// product has one controlling daemon, so serialize this rare workflow to
	// prevent two concurrent setup calls from disagreeing about marker ownership.
	setupRootMu.Lock()
	defer setupRootMu.Unlock()

	if state == nil {
		return domain.SyncRoot{}, fmt.Errorf("state store must not be nil")
	}
	if err := state.ValidateSyncRootCandidate(ctx, root); err != nil {
		return domain.SyncRoot{}, fmt.Errorf("validate sync root candidate: %w", err)
	}
	if err := rootmarker.Ensure(root.LocalRoot, root.UUID); err != nil {
		return domain.SyncRoot{}, fmt.Errorf("establish sync root marker: %w", err)
	}
	stored, err := state.CreateSyncRoot(ctx, root)
	if err != nil {
		// Fail closed. Keep the marker so an uncertain durable outcome can never
		// turn into a configured root that silently lacks its safety identity.
		return domain.SyncRoot{}, fmt.Errorf("register sync root after marker established: %w", err)
	}
	return stored, nil
}
