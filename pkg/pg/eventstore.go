package pg

import (
	"database/sql"
	"fmt"
	"github.com/openyard/evently/pkg/evently"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/openyard/evently/command/es"
	"github.com/openyard/evently/event"
)

var (
	_ es.EventStore            = (*EventStore)(nil)
	_ es.MultiStreamEventStore = (*EventStore)(nil)
	_ es.Transport             = (*EventStore)(nil)
)

type EventStore struct {
	db                  *sql.DB
	batchMode, bulkMode bool
	compressionEnabled  bool
}

type EventStoreOption func(_es *EventStore)

func NewEventStore(db *sql.DB, opts ...EventStoreOption) *EventStore {
	_es := &EventStore{db: db}
	change := NewChange()
	if err := change.Install(db, esChanges); err != nil {
		panic(err)
	}
	for _, opt := range opts {
		opt(_es)
	}
	return _es
}

func WithBatchMode() EventStoreOption {
	return func(_es *EventStore) {
		_es.batchMode = true
		_es.bulkMode = false
	}
}

func WithBulkMode() EventStoreOption {
	return func(_es *EventStore) {
		_es.batchMode = false
		_es.bulkMode = true
	}
}

func WithCompression() EventStoreOption {
	return func(_es *EventStore) {
		_es.compressionEnabled = true
	}
}

func (_es *EventStore) ReadStream(stream string) (es.History, error) {
	events := make([]*event.Event, 0)
	rows, err := _es.queryStream(stream)
	if err != nil {
		log.Printf("could not select stream from database: %s", err.Error())
		return events, evently.Errorf(es.ErrReadStreamFailed, "%s", stream)
	}
	defer func() {
		_ = rows.Close()
	}()
	return _es.rows2events(rows), nil
}

func (_es *EventStore) ReadStreamAt(stream string, at time.Time) (es.History, error) {
	events := make([]*event.Event, 0)
	rows, err := _es.queryStreamAt(stream, at)
	if err != nil {
		log.Printf("could not select stream from database: %s", err.Error())
		return events, evently.Errorf(es.ErrReadStreamFailed, "%s", stream)
	}
	defer func() {
		_ = rows.Close()
	}()
	return _es.rows2events(rows), nil
}

func (_es *EventStore) ReadStreams(stream []string) (map[string]es.History, error) {
	//TODO implement me
	panic("implement me")
}

func (_es *EventStore) AppendToStream(stream string, expectedVersion uint64, events ...*event.Event) error {
	start := time.Now()
	defer func() {
		if os.Getenv("TRACE") != "" {
			log.Printf("[TRACE][%T] AppendToStream: (%s) took %s", _es, stream, time.Since(start))
		}
	}()
	tx, err := _es.db.Begin()
	if err != nil {
		return err
	}
	if _es.batchMode {
		if err := _es.batchInsert(tx, stream, expectedVersion, events); err != nil {
			_ = tx.Rollback()
			return err
		}
	} else if _es.bulkMode {
		if err := _es.bulkInsert(tx, stream, expectedVersion, events); err != nil {
			_ = tx.Rollback()
			return err
		}
	} else {
		for _, e := range events {
			if err := _es.saveEvent(tx, stream, e, expectedVersion); err != nil {
				_ = tx.Rollback()
				return err
			}
			expectedVersion++
		}
	}

	return tx.Commit()
}

func (_es *EventStore) AppendToStreams(streams map[string][]es.Change) error {
	start := time.Now()
	defer func() {
		if os.Getenv("TRACE") != "" {
			log.Printf("[TRACE][%T] AppendToStreams: (%d) took %s", _es, len(streams), time.Since(start))
		}
	}()
	tx, err := _es.db.Begin()
	if err != nil {
		return err
	}
	if _es.batchMode {
		const COLUMNS = 7
		valueStrings := make([]string, 0)
		valueArgs := make([]interface{}, 0)
		var rowsExpected int64
		i := 0
		for stream, events := range streams {
			for _, change := range events {
				evt := change.Event()
				expectedVersion := change.Version()
				valueStrings = append(valueStrings, fmt.Sprintf(
					"($%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*COLUMNS+1, i*COLUMNS+2, i*COLUMNS+3, i*COLUMNS+4, i*COLUMNS+5, i*COLUMNS+6, i*COLUMNS+7))
				valueArgs = append(valueArgs, stream)
				valueArgs = append(valueArgs, expectedVersion+uint64(i)+1)
				valueArgs = append(valueArgs, evt.AggregateID())
				valueArgs = append(valueArgs, evt.ID())
				valueArgs = append(valueArgs, evt.Name())
				valueArgs = append(valueArgs, evt.OccurredAt().UTC().Format(time.RFC3339Nano))
				valueArgs = append(valueArgs, evt.Payload())
				expectedVersion++
				rowsExpected++
				i++
			}
		}
		stmt := fmt.Sprintf(batchInsertStmt, strings.Join(valueStrings, `,`))
		result, err := tx.Exec(stmt, valueArgs...)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		count, err := result.RowsAffected()
		if count != rowsExpected {
			log.Printf("[ERROR][%T] batchInsert: insert into EVENTS effected %d rows - expected %d.", _es, count, rowsExpected)
			return fmt.Errorf("[ERROR][%T] batchInsert: %d: %s", _es, es.ErrConcurrentChange, "ErrConcurrentChange")
		}
		log.Printf("[TRACE][%T] batchInsert done: streams=%d, events=%d)", _es, len(streams), count)
	} else if _es.bulkMode {
		var streamNames, aggregateIDs, eventIDs, eventNames, eventOccurredAt []string
		var streamVersions []uint64
		var eventPayloads [][]byte
		var rowsExpected int64
		for streamName, events := range streams {
			i := 0
			for _, change := range events {
				evt := change.Event()
				expectedVersion := change.Version()
				streamNames = append(streamNames, streamName)
				streamVersions = append(streamVersions, expectedVersion+uint64(i))
				aggregateIDs = append(aggregateIDs, evt.AggregateID())
				eventIDs = append(eventIDs, evt.ID())
				eventNames = append(eventNames, evt.Name())
				eventOccurredAt = append(eventOccurredAt, evt.OccurredAt().UTC().Format(time.RFC3339Nano))
				eventPayloads = append(eventPayloads, evt.Payload())
				rowsExpected++
			}
		}
		result, err := tx.Exec(bulkInsertStmt, pq.Array(streamNames), pq.Array(streamVersions), pq.Array(aggregateIDs), pq.Array(eventIDs), pq.Array(eventNames), pq.Array(eventOccurredAt), pq.Array(eventPayloads))
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if count != rowsExpected {
			log.Printf("[ERROR][%T] bulkInsert: insert into EVENTS effected %d rows - expected %d.", _es, count, rowsExpected)
			return fmt.Errorf("[ERROR][%T] bulkInsert: %d: %s", _es, es.ErrConcurrentChange, "ErrConcurrentChange")
		}
		log.Printf("[TRACE][%T] bulkInsert done: streams=%d, events=%d)", _es, len(streams), count)
	} else {
		return evently.Errorf(es.ErrMisconfiguration, "ErrMisconfiguration", "[ERROR][%T] no batch or bulk mode set for append multiple streams", _es)
	}
	return tx.Commit()
}

func (_es *EventStore) Subscribe() <-chan []*es.Entry {
	//TODO implement me
	panic("implement me")
}

func (_es *EventStore) SubscribeWithOffset(offset uint64) <-chan []*es.Entry {
	//TODO implement me
	panic("implement me")
}

func (_es *EventStore) queryStream(name string) (*sql.Rows, error) {
	return _es.db.Query("select GLOBAL_POSITION, STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT "+
		"from EVENTS where STREAM_NAME = $1 order by EVENT_STAMP ASC", name,
	)
}

func (_es *EventStore) queryStreamAt(name string, upTo time.Time) (*sql.Rows, error) {
	return _es.db.Query("select GLOBAL_POSITION, STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT "+
		"from EVENTS where STREAM_NAME = $1 and EVENT_OCCURRED_AT < $2 order by EVENT_STAMP ASC", name, upTo.Format(time.RFC3339Nano),
	)
}

func (_es *EventStore) rows2events(rows *sql.Rows) []*event.Event {
	stream := make([]*event.Event, 0)
	for rows.Next() {
		var (
			streamID    string
			aggregateID string
			ID          string
			name        string
			timestamp   string
			payload     []byte
		)
		if err := rows.Scan(&streamID, &aggregateID, &ID, &name, &timestamp, &payload); err != nil {
			log.Printf("could not fetch events from database: %s", err.Error())
			return stream
		}
		occurredAt, err := time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			log.Printf("[%T] rows2events: couldn't parse timestamp(%s), err: %s", _es, timestamp, err)
			return stream
		}
		opts := []event.Option{event.WithID(ID), event.WithEventType(event.DomainEvent), event.WithPayload(payload)}
		stream = append(stream, event.NewEventAt(name, aggregateID, occurredAt, opts...))
	}
	return stream
}

func (_es *EventStore) saveEvent(tx *sql.Tx, stream string, event *event.Event, expectedVersion uint64) error {
	result, err := tx.Exec(
		"insert into EVENTS (STREAM_NAME, STREAM_VERSION, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT) values ($1, $2, $3, $4, $5, $6, $7)",
		stream, expectedVersion+1, event.AggregateID(), event.ID(), event.Name(), event.OccurredAt().Format(time.RFC3339Nano), event.Payload(),
	)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		log.Printf("insert into EVENTS effected %d rows - exepcted 1.", count)
		return evently.Errorf(es.ErrConcurrentChange, "%s", stream)
	}
	log.Printf("[TRACE][%T] saveEvent done: stream=%s, version=%d, aggregateID=%s, eventID=%s, eventName=%s) done",
		_es, stream, expectedVersion+1, event.AggregateID(), event.ID(), event.Name())
	return nil
}

func (_es *EventStore) batchInsert(tx *sql.Tx, stream string, expectedVersion uint64, events []*event.Event) error {
	valueStrings := make([]string, 0, len(events))
	valueArgs := make([]interface{}, 0, len(events)*7)
	i := 0
	for _, next := range events {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7))
		valueArgs = append(valueArgs, stream)
		valueArgs = append(valueArgs, expectedVersion+1)
		valueArgs = append(valueArgs, next.AggregateID())
		valueArgs = append(valueArgs, next.ID())
		valueArgs = append(valueArgs, next.Name())
		valueArgs = append(valueArgs, next.OccurredAt().UTC().Format(time.RFC3339Nano))
		valueArgs = append(valueArgs, next.Payload())
		expectedVersion++
		i++
	}
	stmt := fmt.Sprintf(batchInsertStmt, strings.Join(valueStrings, `,`))
	result, err := tx.Exec(stmt, valueArgs...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if count != int64(len(events)) {
		log.Printf("[ERROR][%T] batchInsert: insert into EVENTS effected %d rows - expected %d.", _es, count, len(events))
		return fmt.Errorf("[ERROR][%T] batchInsert: %d: %s", _es, es.ErrConcurrentChange, "ErrConcurrentChange")
	}
	log.Printf("[TRACE][%T] batchInsert done: stream=%s, events=%d)", _es, stream, count)
	return nil
}

func (_es *EventStore) bulkInsert(tx *sql.Tx, stream string, expectedVersion uint64, events []*event.Event) error {
	var streams, aggregateIDs, eventIDs, eventNames, eventOccurredAt []string
	var streamVersions []uint64
	var eventPayloads [][]byte
	for i, e := range events {
		streams = append(streams, stream)
		streamVersions = append(streamVersions, expectedVersion+uint64(i))
		aggregateIDs = append(aggregateIDs, e.AggregateID())
		eventIDs = append(eventIDs, e.ID())
		eventNames = append(eventNames, e.Name())
		eventOccurredAt = append(eventOccurredAt, e.OccurredAt().UTC().Format(time.RFC3339Nano))
		eventPayloads = append(eventPayloads, e.Payload())
	}

	result, err := tx.Exec(bulkInsertStmt, pq.Array(streams), pq.Array(streamVersions), pq.Array(aggregateIDs), pq.Array(eventIDs), pq.Array(eventNames), pq.Array(eventOccurredAt), pq.Array(eventPayloads))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if count != int64(len(events)) {
		log.Printf("[ERROR][%T] bulkInsert: insert into EVENTS effected %d rows - expected %d.", _es, count, len(events))
		return fmt.Errorf("[ERROR][%T] bulkInsert: %d: %s", _es, es.ErrConcurrentChange, "ErrConcurrentChange")
	}
	log.Printf("[TRACE][%T] bulkInsert done: stream=%s, events=%d) done", _es, stream, count)
	return nil
}

var esChanges = ChangeSet{
	"2024-01-07-001_create_events": []string{
		`create table if not exists EVENTS (
    			GLOBAL_POSITION bigint generated always as identity,
				STREAM_NAME varchar(99) not null,
				STREAM_VERSION bigint not null,
				AGGREGATE_ID varchar(99) not null,
				EVENT_ID varchar(99) not null,
				EVENT_NAME varchar(99) not null,
				EVENT_OCCURRED_AT varchar(35) not null,
				EVENT bytea not null,
				EVENT_JSONB jsonb,
    			EVENT_META_DATA jsonb,
                constraint PK_EVENTS primary key (STREAM_NAME, STREAM_VERSION)
			)`,
	},
}

const (
	batchInsertStmt = `insert into EVENTS(STREAM_NAME, STREAM_VERSION, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT) values %s`
	bulkInsertStmt  = `insert into EVENTS(STREAM_NAME, STREAM_VERSION, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT) (
    					select * from unnest($1::text[], $2::int[], $3::text[], $4::text[], $5::text[], $6::text[], $7::bytea[]))`
)

/*
package pgstore

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"scm.sys.flatex.com/golang/cqrs/v2"
	"scm.sys.flatex.com/golang/cqrs/v2/es"
	"scm.sys.flatex.com/golang/db/ddl"
)

type PgStore struct {
	db  *sql.DB
	seq uint64
}

func New(db *sql.DB) *PgStore {
	change := ddl.NewChange()
	pg := ddl.NewPostgres(db)
	if err := change.Install(pg, changes); err != nil {
		panic(err)
	}
	return &PgStore{db: db}
}

func (_es *PgStore) ReadStream(name string) ([]*cqrs.Event, error) {
	events := make([]*cqrs.Event, 0)
	rows, err := _es.queryStream(name)
	if err != nil {
		log.Printf("could not select stream from database: %s", err.Error())
		return events, es.Errorf(es.ErrReadStreamFailed, "%s", name)
	}
	defer rows.Close()
	return _es.rows2events(rows), nil
}

func (_es *PgStore) AppendToStream(name string, events []*cqrs.Event, expectedVersion uint64) error {
	tx, err := _es.db.Begin()
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := _es.saveEvent(tx, name, event, expectedVersion); err != nil {
			_ = tx.Rollback()
			return err
		}
		expectedVersion++
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (_es *PgStore) ReleaseEvent(e *cqrs.Event) {
	tx, err := _es.db.Begin()
	if err != nil {
		return
	}
	if err := _es.releaseEvent(tx, e.ID); err != nil {
		_ = tx.Rollback()
		return
	}

	_ = tx.Commit()
}

func (_es *PgStore) listen() []*cqrs.Event {
	events := make([]*cqrs.Event, 0)
	rows, err := _es.queryUnpublished()
	if err != nil {
		log.Printf("could not select unpublished events from database: %s", err.Error())
		return events
	}
	defer rows.Close()
	for _, e := range _es.rows2events(rows) {
		events = append(events, e)
	}
	return events
}

func (_es *PgStore) queryStream(name string) (*sql.Rows, error) {
	return _es.db.Query("select STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_STAMP, EVENT "+
		"from EVENTS where STREAM_NAME = $1 order by EVENT_STAMP ASC", name,
	)
}

func (_es *PgStore) queryUnpublished() (*sql.Rows, error) {
	return _es.db.Query("select STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_STAMP, EVENT " +
		"from EVENTS where EVENT_PUBLISHED is null order by EVENT_STAMP ASC",
	)
}

func (_es *PgStore) rows2events(rows *sql.Rows) []*cqrs.Event {
	stream := make([]*cqrs.Event, 0)
	for rows.Next() {
		var (
			streamID    string
			aggregateID string
			ID          string
			name        string
			timestamp   string
			payload     []byte
		)
		if err := rows.Scan(&streamID, &aggregateID, &ID, &name, &timestamp, &payload); err != nil {
			log.Printf("could not fetch events from database: %s", err.Error())
			return stream
		}
		stamp, _ := time.Parse(time.RFC3339Nano, timestamp)
		stream = append(stream, &cqrs.Event{Name: name, ID: ID, AggregateID: streamID, Stamp: stamp, Payload: payload})
	}
	return stream
}

func (_es *PgStore) saveEvent(tx *sql.Tx, name string, event *cqrs.Event, expectedVersion uint64) error {
	result, err := tx.Exec(
		"insert into EVENTS (STREAM_NAME, STREAM_VERSION, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_STAMP, EVENT) values ($1, $2, $3, $4, $5, $6, $7)",
		name, expectedVersion+1, event.AggregateID, event.ID, event.Name, event.Stamp.Format(time.RFC3339Nano), event.Payload,
	)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		log.Printf("insert into EVENTS effected %d rows - exepcted 1.", count)
		return fmt.Errorf("saveEvent: %s", "cqrs.ErrOptimisticLockFailed")
	}
	return nil
}

func (_es *PgStore) releaseEvent(tx *sql.Tx, ID string) error {
	result, err := tx.Exec(
		"update EVENTS set EVENT_PUBLISHED = $1 where EVENT_ID = $2 and EVENT_PUBLISHED is null",
		time.Now().UTC().Format(time.RFC3339Nano), ID,
	)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		log.Printf("update EVENTS effected %d rows - exepcted 1.", count)
		return fmt.Errorf("releaseEvent: %s", "cqrs.ErrOptimisticLockFailed")
	}
	return nil
}

var changes = ddl.ChangeSet{
	"2019-10-22-001_create_events": {
		ddl.POSTGRES: {
			`create table if not exists EVENTS (
				STREAM_NAME varchar(99) not null,
				STREAM_VERSION bigint not null,
				AGGREGATE_ID varchar(99) not null,
				EVENT_ID varchar(99) not null,
				EVENT_NAME varchar(99) not null,
				EVENT_STAMP varchar(35) not null,
				EVENT_PUBLISHED varchar(35) null,
				EVENT bytea not null,
                constraint PK_EVENTS primary key (STREAM_NAME, STREAM_VERSION)
			)`,
		},
	},
}
*/
