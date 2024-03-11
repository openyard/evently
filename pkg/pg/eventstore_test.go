package pg_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/pg"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestEventStore_AppendToStream(t *testing.T) {
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
	_es := pg.NewEventStore(_db)
	err = _es.AppendToStream("my-stream", 0,
		event.NewDomainEvent("my-first-event", "4711"),
		event.NewDomainEvent("my-second-event", "4711"),
		event.NewDomainEvent("my-third-event", "4711"))
	if err != nil {
		t.Error(err)
	}
	/*
		"select GLOBAL_POSITION, STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT "+
				"from EVENTS where STREAM_NAME = $1 order by EVENT_STAMP ASC"
	*/

	printEvents(t, err, _db)
}

func TestEventStore_AppendToStreamBatch(t *testing.T) {
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
	err = _es.AppendToStream("my-stream", 0,
		event.NewDomainEvent("my-first-event", "4711"),
		event.NewDomainEvent("my-second-event", "4711"),
		event.NewDomainEvent("my-third-event", "4711"))
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func TestEventStore_AppendToStreamBulk(t *testing.T) {
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
	err = _es.AppendToStream("my-stream", 0,
		event.NewDomainEvent("my-first-event", "4711"),
		event.NewDomainEvent("my-second-event", "4711"),
		event.NewDomainEvent("my-third-event", "4711"))
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func TestEventStore_AppendToStreamsBatch(t *testing.T) {
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
	streams := createStreams()
	err = _es.AppendToStreams(streams)
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func TestEventStore_AppendToStreamsBulk(t *testing.T) {
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
	streams := createStreams()
	err = _es.AppendToStreams(streams)
	if err != nil {
		t.Error(err)
	}
	printEvents(t, err, _db)
}

func createStreams() map[string][]es.Change {
	streams := make(map[string][]es.Change)
	for i := 0; i < 1000; i++ {
		changes := []es.Change{
			es.NewChange(1, event.NewDomainEvent("my-first-event", "4711")),
			es.NewChange(2, event.NewDomainEvent("my-second-event", "4711")),
			es.NewChange(3, event.NewDomainEvent("my-third-event", "4711")),
		}
		streams[fmt.Sprintf("my-stream-%d", i+1)] = changes
	}
	return streams
}

func printEvents(t *testing.T, err error, _db *sql.DB) {
	/*
		"select GLOBAL_POSITION, STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT "+
				"from EVENTS where STREAM_NAME = $1 order by EVENT_STAMP ASC"
	*/

	rows, err := _db.Query(`select * from EVENTS`)
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
		testcontainers.WithImage("docker.io/postgres:15.2-alpine"),
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
