package pg

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/openyard/evently/event"
	"github.com/openyard/evently/pkg/evently"
	"github.com/openyard/evently/tact/es"
)

var (
	_ es.EventStore = (*EventStore)(nil)
	_ es.Transport  = (*EventStore)(nil)
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

func (_es *EventStore) Read(streams ...string) ([]es.Stream, error) {
	res := make([]es.Stream, 0)
	rows, err := _es.queryStreams(streams...)
	if err != nil {
		log.Printf("could not select stream from database: %s", err.Error())
		return res, evently.Errorf(es.ErrReadStreamsFailed, "ErrReadStreamsFailed", "%s", streams)
	}
	_ = rows.Close()
	defer func() {
		_ = rows.Close()
	}()
	return _es.rows2streams(rows), nil
}

func (_es *EventStore) ReadAt(at time.Time, streams ...string) ([]es.Stream, error) {
	res := make([]es.Stream, 0)
	rows, err := _es.queryStreamsAt(at, streams...)
	if err != nil {
		log.Printf("could not select streams from database: %s", err.Error())
		return res, evently.Errorf(es.ErrReadStreamsFailed, "ErrReadStreamsFailed", "%s", streams)
	}
	defer func() {
		_ = rows.Close()
	}()
	return _es.rows2streams(rows), nil
}

func (_es *EventStore) Append(changes ...es.Change) error {
	start := time.Now()
	defer func() {
		if os.Getenv("TRACE") != "" {
			log.Printf("[TRACE][%T] Append: (%d) took %s", _es, len(changes), time.Since(start))
		}
	}()
	streams := make(map[string]map[uint64][]*event.Event)
	for _, c := range changes {
		if _, ok := streams[c.Stream()]; !ok {
			streams[c.Stream()] = make(map[uint64][]*event.Event)
			streams[c.Stream()][c.ExpectedVersion()] = make([]*event.Event, 0)
		}
		streams[c.Stream()][c.ExpectedVersion()] = append(streams[c.Stream()][c.ExpectedVersion()], c.Events()...)
	}
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
		for stream, newEntries := range streams {
			for expectedVersion, events := range newEntries {
				for _, evt := range events {
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
		for stream, newEntries := range streams {
			for expectedVersion, events := range newEntries {
				i := 0
				for _, evt := range events {
					streamNames = append(streamNames, stream)
					streamVersions = append(streamVersions, expectedVersion+uint64(i))
					aggregateIDs = append(aggregateIDs, evt.AggregateID())
					eventIDs = append(eventIDs, evt.ID())
					eventNames = append(eventNames, evt.Name())
					eventOccurredAt = append(eventOccurredAt, evt.OccurredAt().UTC().Format(time.RFC3339Nano))
					eventPayloads = append(eventPayloads, evt.Payload())
					rowsExpected++
					i++
				}
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

func (_es *EventStore) Subscribe(limit uint16) chan []*es.Entry {
	return _es.SubscribeWithOffset(0, limit) // use maxPos()
}

func (_es *EventStore) SubscribeWithOffset(offset uint64, limit uint16) chan []*es.Entry {
	evently.DEBUG("SubscribeWithOffset: offset=%d, limit=%d", offset, limit)
	res := make(chan []*es.Entry)
	rows, err := _es.queryEntries(offset, limit)
	if err != nil {
		log.Printf("[ERROR]\t%T.Subscribe - could not select stream from database: %s", _es, err.Error())
		return res
	}
	go _es.rows2entries(rows, res, func() error {
		return rows.Close()
	})
	return res
}

func (_es *EventStore) SubscribeWithID(ID string, limit uint16) chan []*es.Entry {
	panic("persistent subscriptions not supported yet")
}

func (_es *EventStore) queryStreams(name ...string) (*sql.Rows, error) {
	return _es.db.Query("select GLOBAL_POSITION, STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT "+
		"from EVENTS where STREAM_NAME in (SELECT unnest($1::text[])) order by EVENT_OCCURRED_AT ASC", name,
	)
}

func (_es *EventStore) queryStreamsAt(upTo time.Time, name ...string) (*sql.Rows, error) {
	return _es.db.Query("select GLOBAL_POSITION, STREAM_NAME, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT "+
		"from EVENTS where STREAM_NAME in (SELECT unnest($1::text[])) and EVENT_OCCURRED_AT < $2 order by EVENT_OCCURRED_AT ASC", name, upTo.Format(time.RFC3339Nano),
	)
}

func (_es *EventStore) queryEntries(offset uint64, limit uint16) (*sql.Rows, error) {
	return _es.db.Query("select GLOBAL_POSITION, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT "+
		"from EVENTS where GLOBAL_POSITION >= $1 order by EVENT_OCCURRED_AT ASC limit $2", offset, limit,
	)
}

func (_es *EventStore) rows2streams(rows *sql.Rows) []es.Stream {
	streams := make([]es.Stream, 0)
	events := make(map[string][]*event.Event)
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
			log.Printf("[ERROR]\t%T.rows2streams: could not fetch events from database: %s", _es, err.Error())
			return streams
		}
		occurredAt, err := time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			log.Printf("[ERROR]\t%T.rows2streams: couldn't parse timestamp(%s), err: %s", _es, timestamp, err)
			return streams
		}
		if _, ok := events[streamID]; !ok {
			// first event of stream
			events[streamID] = make([]*event.Event, 0)
		}
		opts := []event.Option{event.WithID(ID), event.WithEventType(event.DomainEvent), event.WithPayload(payload)}
		events[streamID] = append(events[streamID], event.NewEventAt(name, aggregateID, occurredAt, opts...))
	}
	for k, v := range events {
		streams = append(streams, es.BuildStream(k, v...))
	}
	return streams
}

func (_es *EventStore) rows2entries(rows *sql.Rows, res chan []*es.Entry, onFinished func() error) {
	defer onFinished()
	events := make(map[uint64]*event.Event)
	for rows.Next() {
		var (
			globalPos   uint64
			aggregateID string
			ID          string
			name        string
			timestamp   string
			payload     []byte
		)
		if err := rows.Scan(&globalPos, &aggregateID, &ID, &name, &timestamp, &payload); err != nil {
			log.Printf("[ERROR]\t%T.rows2entries: could not fetch events from database: %s", _es, err.Error())
			return
		}
		occurredAt, err := time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			log.Printf("[ERROR]\t%T.rows2entries: couldn't parse timestamp(%s), err: %s", _es, timestamp, err)
			return
		}
		opts := []event.Option{event.WithID(ID), event.WithEventType(event.DomainEvent), event.WithPayload(payload)}
		events[globalPos] = event.NewEventAt(name, aggregateID, occurredAt, opts...)
	}
	entries := make([]*es.Entry, 0)
	for k, v := range events {
		entries = append(entries, es.NewEntry(k, v))
	}
	evently.DEBUG("len(entries)=%d", len(entries))
	res <- entries
}

func (_es *EventStore) saveEvent(tx *sql.Tx, stream string, event *event.Event, expectedVersion uint64) error {
	result, err := tx.Exec(
		"insert into EVENTS (STREAM_NAME, STREAM_POSITION, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT) values ($1, $2, $3, $4, $5, $6, $7)",
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
				STREAM_POSITION bigint not null,
				AGGREGATE_ID varchar(99) not null,
				EVENT_ID varchar(99) not null,
				EVENT_NAME varchar(99) not null,
				EVENT_OCCURRED_AT varchar(35) not null,
				EVENT bytea not null,
				EVENT_JSONB jsonb,
    			EVENT_META_DATA jsonb,
                constraint PK_EVENTS primary key (STREAM_NAME, STREAM_POSITION)
			)`,
	},
}

const (
	batchInsertStmt = `insert into EVENTS(STREAM_NAME, STREAM_POSITION, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT) values %s`
	bulkInsertStmt  = `insert into EVENTS(STREAM_NAME, STREAM_POSITION, AGGREGATE_ID, EVENT_ID, EVENT_NAME, EVENT_OCCURRED_AT, EVENT) (
    					select * from unnest($1::text[], $2::int[], $3::text[], $4::text[], $5::text[], $6::text[], $7::bytea[]))`
)
