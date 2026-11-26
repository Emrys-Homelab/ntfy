package server

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"heckel.io/ntfy/v2/message"
	"heckel.io/ntfy/v2/model"
)

// TestServer_MessageAPI_DeleteRemovesReadMessage is the contract a resolution relies on: DELETE
// means gone. The message is published, marked read (the ack hook the phone calls when the operator
// opens or dismisses it), and then deleted - the case that used to be log-only and left the row in
// the topic cache. After the delete the cache must hold no message row for the identity, only the
// message_delete event clients need, and the registry must answer with a retracted tombstone.
func TestServer_MessageAPI_DeleteRemovesReadMessage(t *testing.T) {
	forEachBackend(t, func(t *testing.T, databaseURL string) {
		c := newTestConfig(t, databaseURL)
		c.MessageRegistryFile = filepath.Join(t.TempDir(), "registry.db")
		s := newTestServer(t, c)

		response := request(t, s, "POST", "/", `{"topic":"mytopic","title":"WARNING: thing","message":"it broke","priority":3,"sequence_id":"seq-1"}`, map[string]string{"Content-Type": "application/json"})
		require.Equal(t, 200, response.Code)
		require.Equal(t, "seq-1", toMessage(t, response.Body.String()).SequenceID)

		response = request(t, s, "POST", "/v1/messages/mytopic/seq-1/ack", "", nil)
		require.Equal(t, 200, response.Code)

		messages := messageAPIMessages(t, s, "mytopic")
		require.Len(t, messages, 2) // the message and the message_clear the ack published
		require.Equal(t, model.MessageEvent, messages[0].Event)

		response = request(t, s, "DELETE", "/v1/messages/mytopic/seq-1", "", nil)
		require.Equal(t, 200, response.Code)
		var deleted messageAPIResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &deleted))
		require.Equal(t, "removed", deleted.Disposition)
		// Only the message row goes: the message_clear the ack published stays, with the new
		// message_delete, so a client replaying the topic still learns what happened.
		require.EqualValues(t, 1, deleted.RemovedRows)
		require.Equal(t, message.RegistryStateRetracted, deleted.State)

		messages = messageAPIMessages(t, s, "mytopic")
		for _, m := range messages {
			require.NotEqual(t, model.MessageEvent, m.Event, "the deleted message must not remain in the cache")
		}

		// The identity is still addressable as a tombstone, so the client learns it was retracted.
		response = request(t, s, "GET", "/v1/messages/mytopic/seq-1", "", nil)
		require.Equal(t, 200, response.Code)
		var state messageAPIResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &state))
		require.Equal(t, message.RegistryStateRetracted, state.State)

		// A replayed DELETE is answered without a second event piling up in the cache.
		before := len(messageAPIMessages(t, s, "mytopic"))
		response = request(t, s, "DELETE", "/v1/messages/mytopic/seq-1", "", nil)
		require.Equal(t, 200, response.Code)
		require.Len(t, messageAPIMessages(t, s, "mytopic"), before)
	})
}

// messageAPIMessages reads the topic listing the message API exposes, for assertions about what a
// subscriber would replay.
func messageAPIMessages(t *testing.T, s *Server, topic string) []*model.Message {
	t.Helper()
	response := request(t, s, "GET", "/v1/messages/"+topic, "", nil)
	require.Equal(t, 200, response.Code)
	var list struct {
		Topic    string           `json:"topic"`
		Messages []*model.Message `json:"messages"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &list))
	require.Equal(t, topic, list.Topic)
	return list.Messages
}
