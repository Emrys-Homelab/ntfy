package message

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"heckel.io/ntfy/v2/log"
	"heckel.io/ntfy/v2/model"
	"heckel.io/ntfy/v2/util"
)

// The registry is the durable half of a message identity. The message cache stores bytes for
// replay and forgets them after cache-duration; the registry keeps the identity, the last
// payload, and the read/ack state for message-registry-duration, in its own SQLite file. It is
// deliberately separate from cache.db: a registry write must never touch the stock cache schema,
// and a cache reset must not erase whether a message was read.

const (
	tagMessageRegistry = "message_registry"

	// RegistryStateActive is a message that can still be updated or retracted.
	RegistryStateActive = "active"

	// RegistryStateRetracted is a message that has been withdrawn.
	RegistryStateRetracted = "retracted"

	// registryPruneInterval is how often expired identities are removed. The retention itself is
	// measured in days, so the interval only bounds how long an expired row stays queryable.
	registryPruneInterval = 10 * time.Minute
)

var (
	// ErrMessageNotFound is returned when the identity is unknown, or its retention has elapsed.
	ErrMessageNotFound = errors.New("message not found")

	// ErrMessageRetracted is returned when an operation is not valid on a retracted message.
	ErrMessageRetracted = errors.New("message retracted")
)

// MessageState is one durable message identity: the sequence ID scoped to its topic, the last
// delivered message ID, its revision, and the read/ack and retraction state.
type MessageState struct {
	Topic       string          `json:"topic"`
	Identity    string          `json:"identity"`   // The sequence ID, scoped to the topic
	MessageID   string          `json:"message_id"` // Last delivered message ID (a new one per revision)
	Revision    int64           `json:"revision"`   // 1 on first publish, incremented per update
	State       string          `json:"state"`      // active or retracted
	Read        bool            `json:"read"`       // A client reported it read/acknowledged
	ReadAt      int64           `json:"read_at,omitempty"`
	AckedAt     int64           `json:"acked_at,omitempty"`
	RetractedAt int64           `json:"retracted_at,omitempty"`
	RetractRead bool            `json:"retract_read,omitempty"` // Retracted after it was read: log-only, no delete event
	CreatedAt   int64           `json:"created_at"`
	UpdatedAt   int64           `json:"updated_at"`
	ExpiresAt   int64           `json:"expires_at"`
	Payload     json.RawMessage `json:"payload,omitempty"` // Last delivered message, for updates and the app log
}

// Registry stores durable message identities in SQLite.
type Registry struct {
	db        *sql.DB
	retention time.Duration
	mu        sync.Mutex
	stop      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
}

const registrySchema = `
CREATE TABLE IF NOT EXISTS message_registry (
	topic         TEXT    NOT NULL,
	identity      TEXT    NOT NULL,
	message_id    TEXT    NOT NULL,
	revision      INTEGER NOT NULL DEFAULT 1,
	state         TEXT    NOT NULL DEFAULT 'active',
	read_at       INTEGER NOT NULL DEFAULT 0,
	acked_at      INTEGER NOT NULL DEFAULT 0,
	retracted_at  INTEGER NOT NULL DEFAULT 0,
	retract_read  INTEGER NOT NULL DEFAULT 0,
	created_at    INTEGER NOT NULL,
	updated_at    INTEGER NOT NULL,
	expires_at    INTEGER NOT NULL,
	payload       TEXT    NOT NULL,
	PRIMARY KEY (topic, identity)
);
CREATE INDEX IF NOT EXISTS idx_message_registry_expires ON message_registry (expires_at);
`

// NewRegistry opens (and creates, if needed) the registry file. The retention is how long an
// identity remains addressable after its last publish/update/ack/retract; it is independent of
// the cache's cache-duration ceiling.
func NewRegistry(filename string, retention time.Duration) (*Registry, error) {
	parentDir := filepath.Dir(filename)
	if !util.FileExists(parentDir) {
		return nil, fmt.Errorf("message registry directory %s does not exist or is not accessible", parentDir)
	}
	if retention <= 0 {
		return nil, errors.New("message registry duration must be positive")
	}
	dsn := filename + "?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL"
	d, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	// The registry is small and single-writer by nature. One connection keeps SQLite from
	// returning SQLITE_BUSY under concurrent update/ack/retract requests.
	d.SetMaxOpenConns(1)
	if _, err := d.Exec(registrySchema); err != nil {
		d.Close()
		return nil, fmt.Errorf("cannot create message registry schema: %w", err)
	}
	r := &Registry{
		db:        d,
		retention: retention,
		stop:      make(chan struct{}),
		stopped:   make(chan struct{}),
	}
	r.prune()
	go r.pruneLoop()
	return r, nil
}

func (r *Registry) pruneLoop() {
	defer close(r.stopped)
	ticker := time.NewTicker(registryPruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.prune()
		case <-r.stop:
			return
		}
	}
}

func (r *Registry) prune() {
	r.mu.Lock()
	defer r.mu.Unlock()
	res, err := r.db.Exec(`DELETE FROM message_registry WHERE expires_at < ?`, time.Now().Unix())
	if err != nil {
		log.Tag(tagMessageRegistry).Err(err).Warn("Unable to prune expired message identities")
		return
	}
	if n, err := res.RowsAffected(); err == nil && n > 0 {
		log.Tag(tagMessageRegistry).Debug("Pruned %d expired message identities", n)
	}
}

// Put records a delivered message under its topic and sequence ID. A repeat publish with the same
// identity increments the revision and replaces the stored payload; read state is preserved,
// because an update must not silently mark a read message unread. A retracted identity is not
// resurrected by a stock publish.
func (r *Registry) Put(m *model.Message) (*MessageState, error) {
	if m.SequenceID == "" {
		return nil, errors.New("message has no sequence ID")
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	expires := now + int64(r.retention/time.Second)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.db.Exec(`
		INSERT INTO message_registry
			(topic, identity, message_id, revision, state, created_at, updated_at, expires_at, payload)
		VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?)
		ON CONFLICT (topic, identity) DO UPDATE SET
			message_id = excluded.message_id,
			revision   = message_registry.revision + 1,
			updated_at = excluded.updated_at,
			expires_at = excluded.expires_at,
			payload    = excluded.payload
		WHERE message_registry.state = ?`,
		m.Topic, m.SequenceID, m.ID, RegistryStateActive, now, now, expires, string(payload), RegistryStateActive); err != nil {
		return nil, err
	}
	return r.getLocked(m.Topic, m.SequenceID)
}

// Get returns the current state of an identity.
func (r *Registry) Get(topic, identity string) (*MessageState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getLocked(topic, identity)
}

func (r *Registry) getLocked(topic, identity string) (*MessageState, error) {
	var (
		st          MessageState
		retractRead int
		payload     string
	)
	err := r.db.QueryRow(`
		SELECT topic, identity, message_id, revision, state, read_at, acked_at, retracted_at,
		       retract_read, created_at, updated_at, expires_at, payload
		FROM message_registry WHERE topic = ? AND identity = ?`, topic, identity).Scan(
		&st.Topic, &st.Identity, &st.MessageID, &st.Revision, &st.State, &st.ReadAt, &st.AckedAt,
		&st.RetractedAt, &retractRead, &st.CreatedAt, &st.UpdatedAt, &st.ExpiresAt, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrMessageNotFound
	} else if err != nil {
		return nil, err
	}
	st.Read = st.ReadAt > 0
	st.RetractRead = retractRead != 0
	st.Payload = json.RawMessage(payload)
	return &st, nil
}

// Ack records that a client read/acknowledged the message. It is idempotent and does not change
// the revision or the state; only the read/ack timestamps move.
func (r *Registry) Ack(topic, identity string) (*MessageState, error) {
	now := time.Now().Unix()
	r.mu.Lock()
	defer r.mu.Unlock()
	st, err := r.getLocked(topic, identity)
	if err != nil {
		return nil, err
	} else if st.State == RegistryStateRetracted {
		return nil, ErrMessageRetracted
	} else if st.Read {
		return st, nil
	}
	expires := now + int64(r.retention/time.Second)
	if _, err := r.db.Exec(`
		UPDATE message_registry SET read_at = ?, acked_at = ?, updated_at = ?, expires_at = ?
		WHERE topic = ? AND identity = ?`, now, now, now, expires, topic, identity); err != nil {
		return nil, err
	}
	return r.getLocked(topic, identity)
}

// MarkRetracted withdraws an identity. retractRead says whether it had already been read; that is
// what tells the caller whether a delete event belongs on the wire.
func (r *Registry) MarkRetracted(topic, identity string, retractRead bool) (*MessageState, error) {
	now := time.Now().Unix()
	readFlag := 0
	if retractRead {
		readFlag = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st, err := r.getLocked(topic, identity)
	if err != nil {
		return nil, err
	} else if st.State == RegistryStateRetracted {
		return st, nil
	}
	expires := now + int64(r.retention/time.Second)
	if _, err := r.db.Exec(`
		UPDATE message_registry SET state = ?, retracted_at = ?, retract_read = ?, updated_at = ?, expires_at = ?
		WHERE topic = ? AND identity = ?`, RegistryStateRetracted, now, readFlag, now, expires, topic, identity); err != nil {
		return nil, err
	}
	return r.getLocked(topic, identity)
}

// Close stops the pruner and closes the database.
func (r *Registry) Close() error {
	var err error
	r.closeOnce.Do(func() {
		close(r.stop)
		<-r.stopped
		err = r.db.Close()
	})
	return err
}
