package pg

import (
	"database/sql"

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
	//TODO implement me
	panic("implement me")
}

func (cp *CheckpointStore) StoreCheckpoint(checkpoint *subscription.Checkpoint) {
	//TODO implement me
	panic("implement me")
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
