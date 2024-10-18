package pg_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/pg"
	"github.com/openyard/evently/tact/es"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func _TestEventStore_Append(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_AppendToStream] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBatchMode())
	err = _es.Append(es.NewChange("my-stream", 0,
		event.NewDomainEvent("my-first-event", "4711"),
		event.NewDomainEvent("my-second-event", "4711"),
		event.NewDomainEvent("my-third-event", "4711")))
	if err != nil {
		t.Error(err)
	}
	/*
		"select GLOBAL_POSITION, STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT "+
				"from EVENTS where STREAM_NAME = $1 order by EVENT_STAMP ASC"
	*/

	printEvents(t, err, _db)
}

func TestEventStore_Read(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_Read] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBatchMode())
	err = _es.Append(es.NewChange("my-stream", 0,
		event.NewDomainEvent("my-first-event", "4711"),
		event.NewDomainEvent("my-second-event", "4711"),
		event.NewDomainEvent("my-third-event", "4711")))
	if err != nil {
		t.Error(err)
	}
	streams, err := _es.Read("my-stream")
	if len(streams) != 1 {
		t.Errorf("expected 1 stream, got %d", len(streams))
	}
	if err != nil {
		t.Error(err)
	}
	t.Logf("read stream <%s> in version <%d> with <%d> events", streams[0].Name(), streams[0].Version(), len(streams[0].Events()))
}

func _TestEventStore_AppendBatch_SingleChange(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_AppendToStream] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBatchMode())
	err = _es.Append(es.NewChange("my-stream-test1", 0,
		event.NewDomainEvent("my-first-event", "4711"),
		event.NewDomainEvent("my-second-event", "4711"),
		event.NewDomainEvent("my-third-event", "4711")))
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func _TestEventStore_AppendBatch_MultipleChanges(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_AppendToStream] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBatchMode())
	err = _es.Append(
		es.NewChange("my-stream-test2", 0, event.NewDomainEvent("my-first-event", "4711")),
		es.NewChange("my-stream-test2", 1, event.NewDomainEvent("my-second-event", "4711")),
		es.NewChange("my-stream-test2", 2, event.NewDomainEvent("my-third-event", "4711")))
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func _TestEventStore_AppendBulk_SingleChange(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_AppendToStream] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBulkMode())
	err = _es.Append(es.NewChange("my-stream-test3", 0,
		event.NewDomainEvent("my-first-event", "4711"),
		event.NewDomainEvent("my-second-event", "4711"),
		event.NewDomainEvent("my-third-event", "4711")))
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func _TestEventStore_AppendBulk_MultipleChanges(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_AppendToStream] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBulkMode())
	err = _es.Append(
		es.NewChange("my-stream-test4", 0, event.NewDomainEvent("my-first-event", "4711")),
		es.NewChange("my-stream-test4", 1, event.NewDomainEvent("my-second-event", "4711")),
		es.NewChange("my-stream-test4", 2, event.NewDomainEvent("my-third-event", "4711")))
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func TestEventStore_AppendStreamsBatch_WithSingleChanges(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_AppendToStream] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBatchMode())
	err = _es.Append(createSingleChanges()...)
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func TestEventStore_AppendStreamsBulk_WithSingleChanges(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_AppendToStream] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBulkMode())
	err = _es.Append(createSingleChanges()...)
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func TestEventStore_AppendStreamsBatch_WithBundledChanges(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_AppendToStream] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBatchMode())
	err = _es.Append(createBundledChanges()...)
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func TestEventStore_AppendStreamsBulk_WithBundĺedChanges(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping [TestEventStore_AppendToStream] in testing.Short() mode")
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
	_es := pg.NewEventStore(_db, pg.WithBulkMode())
	err = _es.Append(createBundledChanges()...)
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func createSingleChanges() []es.Change {
	changes := make([]es.Change, 0)
	for i := 0; i < 10; i++ {
		changes = append(changes, []es.Change{
			es.NewChange(fmt.Sprintf("my-stream-%d", i+1), 0, event.NewDomainEvent("my-first-event", "4711")),
			es.NewChange(fmt.Sprintf("my-stream-%d", i+1), 1, event.NewDomainEvent("my-second-event", "4711")),
			es.NewChange(fmt.Sprintf("my-stream-%d", i+1), 2, event.NewDomainEvent("my-third-event", "4711")),
		}...)
	}
	return changes
}

func createBundledChanges() []es.Change {
	changes := make([]es.Change, 0)
	for i := 0; i < 10; i++ {
		changes = append(changes,
			es.NewChange(fmt.Sprintf("my-stream-%d", i+1), 0,
				event.NewDomainEvent("my-first-event", "4711"),
				event.NewDomainEvent("my-second-event", "4711"),
				event.NewDomainEvent("my-third-event", "4711")),
		)
	}
	return changes
}

func printEvents(t *testing.T, err error, _db *sql.DB) {
	if os.Getenv("TRACE") == "" {
		return
	}
	/*
		"select GLOBAL_POSITION, STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT "+
				"from EVENTS where STREAM_NAME = $1 order by EVENT_STAMP ASC"
	*/

	rows, err := _db.Query(`select * from EVENTS order by EVENT_OCCURRED_AT, GLOBAL_POSITION, STREAM_NAME, STREAM_POSITION`)
	if err != nil {
		t.Error(err)
	}
	for rows.Next() {
		var (
			globalPos       string
			streanName      string
			streamVersion   string
			aggregateID     string
			eventID         string
			eventName       string
			eventOccurredAt string
			event           []byte
			eventJSON       []byte
			eventMetaData   []byte
		)
		err = rows.Scan(&globalPos, &streanName, &streamVersion, &aggregateID, &eventID, &eventName, &eventOccurredAt, &event, &eventJSON, &eventMetaData)
		if err != nil {
			t.Error(err)
		}
		log.Printf("%s | %s | %s | %s | %s | %s | %s | %s | %s | %s\n", globalPos, streanName, streamVersion, aggregateID, eventID, eventName, eventOccurredAt, string(event), string(eventJSON), string(eventMetaData))
	}
}

func initTestDB(ctx context.Context) *postgres.PostgresContainer {
	dbName := "testdb"
	dbUser := "testuser"
	dbPassword := "11111"

	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("nexus.flatex.com:8271/library/postgres:15.2-alpine"),
		//testcontainers.WithImage("docker.io/postgres:15.2-alpine"),
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		panic(err)
	}
	return postgresContainer
}

func stopTestDB(ctx context.Context, postgresContainer *postgres.PostgresContainer) {
	if err := postgresContainer.Terminate(ctx); err != nil {
		panic(err)
	}
}
