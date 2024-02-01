package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

const (
	// updateBatchSize is the number of records to update at a time.
	updateBatchSize = 500000
)

func cleanupDTTempData(ctx context.Context, ds datastore.Batching) {
	const dtCleanupTimeout = 10 * time.Minute
	const dtPrefix = "/dagsync/dtsync/pub"

	ctx, cancel := context.WithTimeout(ctx, dtCleanupTimeout)
	defer cancel()

	count, err := deletePrefix(ctx, ds, dtPrefix)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Info("Not enough time to finish data-transfer state cleanup")
			err = ds.Sync(context.Background(), datastore.NewKey(dtPrefix))
			if err != nil {
				log.Error("failed to sync datastore: %s", err)
			}
			return
		}
		log.Error("failed to remove old data-transfer fsm records: %s", err)
		return
	}
	log.Infow("Removed old temporary data-transfer fsm records", "count", count)
}

func deletePrefix(ctx context.Context, ds datastore.Batching, prefix string) (int, error) {
	q := query.Query{
		KeysOnly: true,
		Prefix:   prefix,
	}
	results, err := ds.Query(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("cannot query datastore: %w", err)
	}
	defer results.Close()

	batch, err := ds.Batch(ctx)
	if err != nil {
		return 0, fmt.Errorf("cannot create datastore batch: %w", err)
	}

	var keyCount, writeCount int
	for result := range results.Next() {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if writeCount >= updateBatchSize {
			writeCount = 0
			if err = batch.Commit(ctx); err != nil {
				return 0, fmt.Errorf("cannot commit datastore: %w", err)
			}
			log.Infow("Removed datastore records", "count", keyCount)
		}
		if result.Error != nil {
			return 0, fmt.Errorf("cannot read query result from datastore: %w", result.Error)
		}
		ent := result.Entry
		if len(ent.Key) == 0 {
			log.Warnf("result entry has empty key")
			continue
		}

		if err = batch.Delete(ctx, datastore.NewKey(ent.Key)); err != nil {
			return 0, fmt.Errorf("cannot delete key from datastore: %w", err)
		}
		writeCount++
		keyCount++
	}

	if err = batch.Commit(ctx); err != nil {
		return 0, fmt.Errorf("cannot commit datastore: %w", err)
	}
	if err = ds.Sync(context.Background(), datastore.NewKey(q.Prefix)); err != nil {
		return 0, err
	}

	return keyCount, nil
}
