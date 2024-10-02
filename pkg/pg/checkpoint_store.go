package pg

import (
	"database/sql"
	"log"
	"time"

	"github.com/openyard/evently/async/query/subscription"
)

var _ subscription.CheckpointStore = (*CheckpointStore)(nil)

type CheckpointStore struct {
	db *sql.DB
}

func NewCheckpointStore(db *sql.DB) *CheckpointStore {
	cp := &CheckpointStore{db: db}
	change := NewChange()
	if err := change.Install(db, cpChanges); err != nil {
		panic(err)
	}
	return cp
}

func (cp *CheckpointStore) GetLatestCheckpoint(checkpointID string) *subscription.Checkpoint {
	rows, err := cp.db.Query("select GLOBAL_POSITION, LAST_SEEN_AT from CHECKPOINTS where SUBSCRIPTION_ID = $1", checkpointID)
	if err != nil {
		return nil
	}
	if !rows.Next() {
		return subscription.NewCheckpoint(checkpointID, 0, time.Now())
	}
	var (
		globalPos uint64
		timestamp string
	)
	if err := rows.Scan(&globalPos, &timestamp); err != nil {
		log.Printf("could not get latest checkpoint from database: %s", err.Error())
		return nil
	}
	lastSeenAt, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		log.Printf("[%T] GetLatestCheckpoint: couldn't parse timestamp(%s), err: %s", cp, timestamp, err)
		return nil
	}
	return subscription.NewCheckpoint(checkpointID, globalPos, lastSeenAt)
}

func (cp *CheckpointStore) StoreCheckpoint(checkpoint *subscription.Checkpoint) {
	tx, err := cp.db.Begin()
	if err != nil {
		log.Printf("[ERROR]\t%T.StoreCheckpoint failed: %s", cp, err)
		return
	}
	result, err := tx.Exec(
		`insert into CHECKPOINTS (SUBSCRIPTION_ID, GLOBAL_POSITION, LAST_SEEN_AT) values ($1, $2, $3)
				on conflict on constraint PK_CHECKPOINTS 
				do update set GLOBAL_POSITION = excluded.GLOBAL_POSITION, LAST_SEEN_AT = excluded.LAST_SEEN_AT`,
		checkpoint.ID(), checkpoint.GlobalPosition(), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		log.Printf("[ERROR]\t%T.StoreCheckpoint failed (1): %s", cp, err)
		_ = tx.Rollback()
		return
	}
	count, err := result.RowsAffected()
	if err != nil {
		log.Printf("[ERROR]\t%T.StoreCheckpoint failed (2): %s", cp, err)
		_ = tx.Rollback()
		return
	}
	if count != 1 {
		log.Printf("[ERROR]\t%T.StoreCheckpoint failed (3): %s", cp, err)
		_ = tx.Rollback()
		return
	}
	_ = tx.Commit()
	log.Printf("[TRACE]\t[%T].StoreCheckpoint done: ID=%s, pos=%d, lastSeen=%s",
		cp, checkpoint.ID(), checkpoint.GlobalPosition(), checkpoint.LastSeenAt())
}

var cpChanges = ChangeSet{
	"2024-01-07-002_create_checkpoints": []string{
		`create table if not exists CHECKPOINTS (
    			SUBSCRIPTION_ID varchar(99) not null,
    			GLOBAL_POSITION bigint not null,
				LAST_SEEN_AT varchar(35) not null, 
                constraint PK_CHECKPOINTS primary key (SUBSCRIPTION_ID)
			)`,
	},
}
