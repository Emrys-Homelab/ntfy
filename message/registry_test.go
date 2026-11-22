package message

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"heckel.io/ntfy/v2/model"
)

func TestRegistryLifecycle(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "registry.db")
	r, err := NewRegistry(filename, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	m := model.NewDefaultMessage("mytopic", "first")
	m.SequenceID = "identity-1"
	st, err := r.Put(m)
	if err != nil {
		t.Fatal(err)
	} else if st.Revision != 1 || st.State != RegistryStateActive || st.Read {
		t.Fatalf("unexpected initial state: %+v", st)
	}
	st, err = r.Ack("mytopic", "identity-1")
	if err != nil {
		t.Fatal(err)
	} else if !st.Read || st.ReadAt == 0 {
		t.Fatalf("ack did not mark read: %+v", st)
	}
	m2 := model.NewDefaultMessage("mytopic", "second")
	m2.SequenceID = "identity-1"
	st, err = r.Put(m2)
	if err != nil {
		t.Fatal(err)
	} else if st.Revision != 2 || !st.Read || st.MessageID != m2.ID {
		t.Fatalf("update did not preserve read state: %+v", st)
	}
	if _, err := r.Get("mytopic", "nope"); !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("expected ErrMessageNotFound, got %v", err)
	}
	st, err = r.MarkRetracted("mytopic", "identity-1", true)
	if err != nil {
		t.Fatal(err)
	} else if st.State != RegistryStateRetracted || !st.RetractRead {
		t.Fatalf("unexpected retracted state: %+v", st)
	}
	if _, err := r.Ack("mytopic", "identity-1"); !errors.Is(err, ErrMessageRetracted) {
		t.Fatalf("expected ErrMessageRetracted, got %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// The identity survives a restart because it is in its own SQLite file.
	r, err = NewRegistry(filename, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	st, err = r.Get("mytopic", "identity-1")
	if err != nil {
		t.Fatal(err)
	} else if st.State != RegistryStateRetracted || st.Revision != 2 {
		t.Fatalf("state did not survive reopen: %+v", st)
	}
}
