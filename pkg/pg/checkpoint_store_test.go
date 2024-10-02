package pg_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/openyard/evently/async/query/subscription"
	"github.com/openyard/evently/pkg/pg"
)

func TestCheckpointStore_StoreCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestCheckpointStore_StoreCheckpoint] in testing.Short() mode")
	}
	ctx := context.Background()
	db := initTestDB(ctx)
	defer stopTestDB(ctx, db)

	connectionString, err := db.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Error(err)
	}
	_db, err := sql.Open("postgres", connectionString)
	if err != nil {
		t.Error(err)
	}
	t.Logf("using connection(%s)", connectionString)

	sut := pg.NewCheckpointStore(_db)
	sut.StoreCheckpoint(subscription.NewCheckpoint("test", 33, time.Now()))

	cp := sut.GetLatestCheckpoint("test")
	t.Logf("ID=%s pos=%d, lastSeen=%s", cp.ID(), cp.GlobalPosition(), cp.LastSeenAt())
}
