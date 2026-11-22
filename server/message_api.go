package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"heckel.io/ntfy/v2/message"
	"heckel.io/ntfy/v2/model"
	"heckel.io/ntfy/v2/user"
)

// The extended message API. It is additive: the stock publish/subscribe paths are untouched, and
// the delivery primitives it emits (message_clear, message_delete, and a publish with the same
// sequence ID) are the ones stock ntfy clients already understand. What the fork adds is durable
// server-side identity and state, so update/retract/ack survive the cache TTL and a restart, and
// so an update can be *decided* by the server instead of guessed by a client.
//
//	GET    /v1/messages/<topic>/<identity>        read the durable state (read permission)
//	PATCH  /v1/messages/<topic>/<identity>        update; replace if unread, re-alert if read (write)
//	PUT    /v1/messages/<topic>/<identity>        same as PATCH (write)
//	DELETE /v1/messages/<topic>/<identity>        retract; remove if unread, log-only if read (write)
//	POST   /v1/messages/<topic>/<identity>/ack    mark read/acknowledged (read permission)
//
// <identity> is the message's sequence ID, which is the identity stock ntfy already puts on the
// wire. The message `id` is not an identity: it changes with every revision.

const (
	messageAPIPathPrefix = "/v1/messages/"
)

// messageAPIResponse is a registry state with the operation's outcome attached.
type messageAPIResponse struct {
	*message.MessageState
	Disposition string `json:"disposition,omitempty"`
}

// messageUpdateRequest carries the fields an update may change. Pointers distinguish "not
// provided" from "set to the zero value", so a PATCH can change only the body.
type messageUpdateRequest struct {
	Message     *string          `json:"message"`
	Title       *string          `json:"title"`
	Priority    *int             `json:"priority"`
	Tags        *[]string        `json:"tags"`
	Click       *string          `json:"click"`
	Icon        *string          `json:"icon"`
	Actions     *[]*model.Action `json:"actions"`
	ContentType *string          `json:"content_type"`
}

// handleMessageAPI dispatches the extended message endpoints. Authentication already happened in
// handle(); authorization is by the row's topic, so a token with write access to one topic cannot
// touch another topic's message.
func (s *Server) handleMessageAPI(w http.ResponseWriter, r *http.Request, v *visitor) error {
	if s.messageRegistry == nil {
		return errHTTPNotFound // The feature is off unless message-registry-file is configured
	}
	topic, identity, ack := splitMessageAPIPath(r.URL.Path)
	if topic == "" || identity == "" {
		return errHTTPNotFound
	}
	switch {
	case r.Method == http.MethodGet && !ack:
		if err := s.authorizeMessageTopic(v, topic, user.PermissionRead); err != nil {
			return err
		}
		return s.handleMessageGet(w, topic, identity)
	case r.Method == http.MethodPost && ack:
		if err := s.authorizeMessageTopic(v, topic, user.PermissionRead); err != nil {
			return err
		}
		return s.handleMessageAck(w, r, v, topic, identity)
	case (r.Method == http.MethodPatch || r.Method == http.MethodPut) && !ack:
		if err := s.authorizeMessageTopic(v, topic, user.PermissionWrite); err != nil {
			return err
		}
		return s.handleMessageUpdate(w, r, v, topic, identity)
	case r.Method == http.MethodDelete && !ack:
		if err := s.authorizeMessageTopic(v, topic, user.PermissionWrite); err != nil {
			return err
		}
		return s.handleMessageRetract(w, r, v, topic, identity)
	}
	return errHTTPNotFound
}

// splitMessageAPIPath returns the topic, identity and whether the path ends in /ack.
func splitMessageAPIPath(path string) (string, string, bool) {
	if !messageAPIPathRegex.MatchString(path) {
		return "", "", false
	}
	parts := messageAPIPathRegex.FindStringSubmatch(path)
	ack := parts[3] == "/ack"
	return parts[1], parts[2], ack
}

func (s *Server) authorizeMessageTopic(v *visitor, topic string, perm user.Permission) error {
	if s.userManager == nil {
		return nil // Authentication is not configured; this is the stock anonymous mode
	}
	if s.userManager != nil {
		if err := s.userManager.Authorize(v.User(), topic, perm); err != nil {
			logv(v).Err(err).Debug("Access to message topic %s not authorized", topic)
			return errHTTPForbidden
		}
	}
	return nil
}

func (s *Server) handleMessageGet(w http.ResponseWriter, topic, identity string) error {
	st, err := s.messageRegistry.Get(topic, identity)
	if errors.Is(err, message.ErrMessageNotFound) {
		return errHTTPMessageNotFound
	} else if err != nil {
		return err
	}
	return s.writeJSON(w, &messageAPIResponse{MessageState: st, Disposition: "state"})
}

func (s *Server) handleMessageAck(w http.ResponseWriter, r *http.Request, v *visitor, topic, identity string) error {
	st, err := s.messageRegistry.Get(topic, identity)
	if errors.Is(err, message.ErrMessageNotFound) {
		return errHTTPMessageNotFound
	} else if err != nil {
		return err
	} else if st.State == message.RegistryStateRetracted {
		return errHTTPConflictMessageRetracted
	}
	if st.Read {
		return s.writeJSON(w, &messageAPIResponse{MessageState: st, Disposition: "already_acked"})
	}
	st, err = s.messageRegistry.Ack(topic, identity)
	if err != nil {
		return err
	}
	// The clear event is what makes a later update re-alert rather than replace: stock clients
	// drop the notification slot when they see message_clear, so the next same-sequence message is
	// a new notification. It is also the stock "read" signal, so no new wire primitive is needed.
	if err := s.publishActionMessage(v, topic, identity, model.MessageClearEvent); err != nil {
		return err
	}
	return s.writeJSON(w, &messageAPIResponse{MessageState: st, Disposition: "acknowledged"})
}

func (s *Server) handleMessageUpdate(w http.ResponseWriter, r *http.Request, v *visitor, topic, identity string) error {
	st, err := s.messageRegistry.Get(topic, identity)
	if errors.Is(err, message.ErrMessageNotFound) {
		return errHTTPMessageNotFound
	} else if err != nil {
		return err
	} else if st.State == message.RegistryStateRetracted {
		return errHTTPConflictMessageRetracted
	}
	var req messageUpdateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, jsonBodyBytesLimit)).Decode(&req); err != nil {
		return errHTTPBadRequest
	}
	// Rebuild from the stored payload so a PATCH that omits a field keeps its previous value.
	var m model.Message
	if err := json.Unmarshal(st.Payload, &m); err != nil {
		return err
	}
	if req.Message != nil {
		m.Message = *req.Message
		m.Encoding = "" // The update body is text, not the base64 the original may have used
	}
	if req.Title != nil {
		m.Title = *req.Title
	}
	if req.Priority != nil {
		if *req.Priority < 1 || *req.Priority > 5 {
			return errHTTPBadRequestPriorityInvalid
		}
		m.Priority = *req.Priority
	}
	if req.Tags != nil {
		m.Tags = *req.Tags
	}
	if req.Click != nil {
		m.Click = *req.Click
	}
	if req.Icon != nil {
		m.Icon = *req.Icon
	}
	if req.Actions != nil {
		m.Actions = *req.Actions
	}
	if req.ContentType != nil {
		m.ContentType = *req.ContentType
	}
	if len(m.Message) > s.config.MessageSizeLimit {
		return errHTTPBadRequestMessageTooLarge
	}
	m.ID = model.GenerateMessageID()
	m.SequenceID = identity
	m.Topic = topic
	m.Time = time.Now().Unix()
	m.Event = model.MessageEvent
	m.Expires = time.Unix(m.Time, 0).Add(v.Limits().MessageExpiryDuration).Unix()
	if m.Message == "" {
		m.Message = emptyMessageBody
	}
	m.SanitizeUTF8()

	if d := v.MessageAllowed(); !d.OK {
		return errHTTPTooManyRequestsLimitMessages.WithLimitDecision(d)
	}
	t, err := s.topicFromID(v, topic)
	if err != nil {
		return err
	}
	// Read: clear the existing notification slot first, then publish the update. The client sees
	// clear -> new message, so it alerts again instead of silently replacing. Unread: the update
	// alone replaces the still-displayed notification in place.
	disposition := "replaced"
	if st.Read {
		disposition = "realerted"
		if err := s.publishActionMessage(v, topic, identity, model.MessageClearEvent); err != nil {
			return err
		}
	}
	if err := s.dispatch(v, t, &m, dispatchOpts{firebase: true, webPush: true}); err != nil {
		return err
	}
	if err := s.messageCache.AddMessage(&m); err != nil {
		return err
	}
	s.mu.Lock()
	s.messages++
	s.mu.Unlock()
	st, err = s.messageRegistry.Put(&m)
	if err != nil {
		return err
	}
	w.Header().Set("X-Ntfy-Sequence-ID", identity)
	return s.writeJSON(w, &messageAPIResponse{MessageState: st, Disposition: disposition})
}

func (s *Server) handleMessageRetract(w http.ResponseWriter, r *http.Request, v *visitor, topic, identity string) error {
	st, err := s.messageRegistry.Get(topic, identity)
	if errors.Is(err, message.ErrMessageNotFound) {
		return errHTTPMessageNotFound
	} else if err != nil {
		return err
	} else if st.State == message.RegistryStateRetracted {
		return s.writeJSON(w, &messageAPIResponse{MessageState: st, Disposition: "already_retracted"})
	}
	read := st.Read
	if !read {
		// Unread: the stock delete event removes it from the notification shade. Read: no event is
		// published at all, so it stays a silent log line in the app instead of re-notifying.
		if err := s.publishActionMessage(v, topic, identity, model.MessageDeleteEvent); err != nil {
			return err
		}
	}
	st, err = s.messageRegistry.MarkRetracted(topic, identity, read)
	if err != nil {
		return err
	}
	disposition := "removed"
	if read {
		disposition = "log_only"
	}
	return s.writeJSON(w, &messageAPIResponse{MessageState: st, Disposition: disposition})
}

// publishActionMessage publishes a stock message_clear or message_delete event for an identity.
// It mirrors handleActionMessage: same event, same sequence ID, same cache, same side effects.
func (s *Server) publishActionMessage(v *visitor, topic, identity, event string) error {
	t, err := s.topicFromID(v, topic)
	if err != nil {
		return err
	}
	m := model.NewActionMessage(event, topic, identity)
	m.Sender = v.IP()
	m.User = v.MaybeUserID()
	m.Expires = time.Unix(m.Time, 0).Add(v.Limits().MessageExpiryDuration).Unix()
	if err := s.dispatch(v, t, m, dispatchOpts{firebase: true, webPush: true}); err != nil {
		return err
	}
	if err := s.messageCache.AddMessage(m); err != nil {
		return err
	}
	s.mu.Lock()
	s.messages++
	s.mu.Unlock()
	return nil
}
