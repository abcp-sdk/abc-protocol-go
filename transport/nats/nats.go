package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	abcprotocol "forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/bus"
	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/protocol"
	"github.com/nats-io/nats.go"
)

const (
	streamMailbox   = "ABC_MAILBOX"
	streamEvents    = "ABC_EVENTS"
	streamDLQ       = "ABC_DLQ"
	inboxWildcard   = "abc.mailbox.>"
	eventsWildcard  = "abc.session.events.>"
	dlqWildcard     = "abc.dlq.>"
	durableConsumer = "abc-mailbox-push"
	queueGroup      = "abc-mailbox"
	objectBucket    = "ABC_TOOL"
)

// Bus is the NATS transport adapter.
type Bus struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

var _ bus.Bus = (*Bus)(nil)

// Connect establishes a NATS connection and ensures the mailbox stream.
func Connect(url string) (*Bus, error) {
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}
	// Two streams, two consumption models: the mailbox is a work queue
	// (competing consumers, ack on done), session events are a replayable
	// per-session log. Splitting them lets retention and consumers evolve
	// independently.
	if _, err := js.StreamInfo(streamMailbox); err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamMailbox,
			Subjects: []string{inboxWildcard},
			MaxAge:   24 * time.Hour,
		})
		if err != nil {
			nc.Close()
			return nil, err
		}
	}
	if _, err := js.StreamInfo(streamEvents); err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamEvents,
			Subjects: []string{eventsWildcard},
			MaxAge:   24 * time.Hour,
		})
		if err != nil {
			// SELF-MIGRATION (0.1 -> 0.2): a pre-0.2 ABC_MAILBOX still
			// declares abc.session.events.>, which overlaps. Narrow the
			// legacy stream to its mailbox subjects (mailbox messages are
			// preserved; historical events stay in the old stream, outside
			// the new replay window) and retry. The fleet must never crash
			// on the 0.2 layout migration.
			if strings.Contains(err.Error(), "subjects overlap") {
				if migErr := migrateLegacyMailbox(js); migErr == nil {
					_, err = js.AddStream(&nats.StreamConfig{
						Name:     streamEvents,
						Subjects: []string{eventsWildcard},
						MaxAge:   24 * time.Hour,
					})
				}
			}
			if err != nil {
				nc.Close()
				return nil, err
			}
		}
	}
	// Dead letters: term() copies the message here before terminating it,
	// so poison payloads are inspectable instead of vanishing.
	if _, err := js.StreamInfo(streamDLQ); err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamDLQ,
			Subjects: []string{dlqWildcard},
			MaxAge:   24 * time.Hour,
		})
		if err != nil {
			nc.Close()
			return nil, err
		}
	}
	return &Bus{nc: nc, js: js}, nil
}

// migrateLegacyMailbox narrows a pre-0.2 ABC_MAILBOX (which also captured
// session events) to its mailbox-only subjects so ABC_EVENTS can be created.
func migrateLegacyMailbox(js nats.JetStreamContext) error {
	info, err := js.StreamInfo(streamMailbox)
	if err != nil {
		return err
	}
	hasEvents := false
	for _, s := range info.Config.Subjects {
		if s == eventsWildcard {
			hasEvents = true
			break
		}
	}
	if !hasEvents {
		return fmt.Errorf("overlap not caused by the legacy mailbox layout")
	}
	cfg := info.Config
	cfg.Subjects = []string{inboxWildcard}
	_, err = js.UpdateStream(&cfg)
	return err
}

// streamFor routes a subject to the stream that captures it (subjects are
// disjoint by design; the pre-0.2 single ABC_MAILBOX stream also captured
// session events).
func streamFor(subject string) string {
	if strings.HasPrefix(subject, eventsWildcard[:len(eventsWildcard)-1]) {
		return streamEvents
	}
	if strings.HasPrefix(subject, dlqWildcard[:len(dlqWildcard)-1]) {
		return streamDLQ
	}
	return streamMailbox
}

// dlqSubjectFor maps an original queue subject to its dead-letter subject.
func dlqSubjectFor(subject string) string {
	token := subject
	if i := strings.LastIndex(subject, "."); i >= 0 {
		token = subject[i+1:]
	}
	return dlqWildcard[:len(dlqWildcard)-1] + token
}

func encode(payload any) ([]byte, error) { return json.Marshal(payload) }

func decode(m *nats.Msg) (abcprotocol.Envelope, error) {
	var env abcprotocol.Envelope
	if err := json.Unmarshal(m.Data, &env); err != nil {
		return env, err
	}
	if m.Reply != "" {
		env.ReplyTo = &m.Reply
	}
	return env, nil
}

func (b *Bus) Request(ctx context.Context, ch string, payload any, opts bus.RequestOpts) (abcprotocol.Envelope, error) {
	data, err := encode(map[string]any{"v": 1, "ch": ch, "kind": "req", "payload": payload})
	if err != nil {
		return abcprotocol.Envelope{}, err
	}
	if opts.SessionName != "" {
		data, err = encode(map[string]any{"v": 1, "ch": ch, "kind": "req", "session_name": opts.SessionName, "payload": payload})
		if err != nil {
			return abcprotocol.Envelope{}, err
		}
	}
	// TimeoutMs > 0 bounds the request; 0 means no timeout (ctx only).
	var cancel context.CancelFunc
	if opts.TimeoutMs > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(opts.TimeoutMs)*time.Millisecond)
		defer cancel()
	}
	m, err := b.nc.RequestWithContext(ctx, ch, data)
	if err != nil {
		return abcprotocol.Envelope{}, err
	}
	return decode(m)
}

func (b *Bus) RequestMany(ctx context.Context, ch string, payload any, opts bus.RequestOpts) ([]abcprotocol.Envelope, error) {
	data, err := encode(map[string]any{"v": 1, "ch": ch, "kind": "req", "payload": payload})
	if err != nil {
		return nil, err
	}
	inbox := nats.NewInbox()
	sub, err := b.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	msg := nats.NewMsg(ch)
	msg.Data = data
	msg.Reply = inbox
	if err := b.nc.PublishMsg(msg); err != nil {
		return nil, err
	}

	maxWait := opts.MaxWaitMs
	if maxWait == 0 {
		maxWait = 500
	}
	out := []abcprotocol.Envelope{}
	for {
		m, err := sub.NextMsg(time.Duration(maxWait) * time.Millisecond)
		if err != nil {
			return out, nil
		}
		env, err := decode(m)
		if err != nil {
			continue
		}
		out = append(out, env)
	}
}

func (b *Bus) Publish(ctx context.Context, ch string, payload any, replyTo string) error {
	data, err := encode(map[string]any{"v": 1, "ch": ch, "kind": "pub", "payload": payload})
	if err != nil {
		return err
	}
	msg := nats.NewMsg(ch)
	msg.Data = data
	if replyTo != "" {
		msg.Reply = replyTo
	}
	return b.nc.PublishMsg(msg)
}

func (b *Bus) Subscribe(ctx context.Context, ch string, opts bus.SubscribeOpts) (bus.Subscription, error) {
	var sub *nats.Subscription
	var err error
	if opts.Queue != "" {
		sub, err = b.nc.QueueSubscribeSync(ch, opts.Queue)
	} else {
		sub, err = b.nc.SubscribeSync(ch)
	}
	if err != nil {
		return nil, err
	}
	// SubscribeSync is subject to an async-subscription race: a publish issued
	// right after Subscribe may land before NATS registers interest on the
	// server. Flushing the connection pins the subscription.
	if err := b.nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}
	return &natsSub{sub: sub}, nil
}

type natsSub struct {
	sub *nats.Subscription
}

func (s *natsSub) Next(ctx context.Context) (abcprotocol.Envelope, bool) {
	m, err := s.sub.NextMsgWithContext(ctx)
	if err != nil {
		return abcprotocol.Envelope{}, false
	}
	env, err := decode(m)
	if err != nil {
		return abcprotocol.Envelope{}, false
	}
	return env, true
}

func (s *natsSub) Close() error { return s.sub.Unsubscribe() }

func (b *Bus) InboxPublish(ctx context.Context, ch string, payload any, opts bus.InboxPublishOpts) error {
	data, err := encode(map[string]any{"v": 1, "ch": ch, "kind": "queue", "id": opts.ID, "payload": payload})
	if err != nil {
		return err
	}
	if opts.SessionName != "" {
		data, err = encode(map[string]any{"v": 1, "ch": ch, "kind": "queue", "id": opts.ID, "session_name": opts.SessionName, "payload": payload})
		if err != nil {
			return err
		}
	}
	_, err = b.js.Publish(ch, data, nats.MsgId(opts.ID))
	return err
}

func (b *Bus) InboxConsume(ctx context.Context, opts bus.InboxConsumeOpts) (bus.InboxSubscription, error) {
	subject := opts.Subject
	if subject == "" {
		subject = inboxWildcard
	}
	// An explicit pull consumer per distinct subject filter. A durable
	// consumer's filter_subject is fixed at creation, so reusing one durable
	// name across different subjects silently delivers nothing — derive a
	// stable per-subject durable name instead.
	durable := durableConsumer
	if subject != inboxWildcard {
		durable = durableConsumer + "-" + protocol.SessionToken(subject)
	}
	sub, err := b.js.PullSubscribe(subject, durable,
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		return nil, err
	}
	return &inboxSub{js: b.js, sub: sub}, nil
}

type inboxSub struct {
	js  nats.JetStreamContext
	sub *nats.Subscription
}

func (s *inboxSub) Next(ctx context.Context) (*bus.InboxMsg, bool) {
	// PullSubscription requires Fetch(); NextMsgWithContext rejects pull subs.
	msgs, err := s.sub.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil || len(msgs) == 0 {
		return nil, false
	}
	m := msgs[0]
	env, err := decode(m)
	if err != nil {
		return nil, false
	}
	msg := &bus.InboxMsg{Envelope: env}
	msg.SetHandlers(
		func() { _ = m.Ack() },
		func(delayMs int) { _ = m.NakWithDelay(time.Duration(delayMs) * time.Millisecond) },
		// Term copies the message to the dead-letter stream first; use
		// TermNoDLQ to discard it outright.
		func() {
			if id := env.Id; id != nil {
				_, _ = s.js.Publish(dlqSubjectFor(m.Subject), m.Data, nats.MsgId(*id))
			} else {
				_, _ = s.js.Publish(dlqSubjectFor(m.Subject), m.Data)
			}
			_ = m.Term()
		},
		func() { _ = m.Term() },
	)
	return msg, true
}

func (s *inboxSub) Close() error { return s.sub.Unsubscribe() }

func (b *Bus) ObjectPut(ctx context.Context, name string, data []byte) error {
	os, err := b.js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: objectBucket, TTL: 24 * 3600 * 1e9})
	if err != nil {
		// Bucket exists (possibly with another config) — open it.
		os, err = b.js.ObjectStore(objectBucket)
		if err != nil {
			return err
		}
	}
	_, err = os.PutBytes(name, data)
	return err
}

func (b *Bus) ObjectGet(ctx context.Context, name string) ([]byte, error) {
	os, err := b.js.ObjectStore(objectBucket)
	if err != nil {
		return nil, nil
	}
	return os.GetBytes(name)
}

// kvStore creates-or-opens a KV bucket. CreateKeyValue errors when the
// bucket exists with a different config (e.g. a different per-call TTL), so
// fall back to opening the existing bucket.
func (b *Bus) kvStore(bucket string, ttlMs int64) (nats.KeyValue, error) {
	kv, err := b.js.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucket, TTL: time.Duration(ttlMs) * time.Millisecond})
	if err != nil {
		return b.js.KeyValue(bucket)
	}
	return kv, nil
}

func (b *Bus) KVCreate(ctx context.Context, bucket, key, value string, ttlMs int64) (int64, error) {
	kv, err := b.kvStore(bucket, ttlMs)
	if err != nil {
		return 0, err
	}
	rev, err := kv.Create(key, []byte(value))
	if err != nil {
		return 0, nil
	}
	return int64(rev), nil
}

func (b *Bus) KVPut(ctx context.Context, bucket, key, value string, ttlMs int64) error {
	kv, err := b.kvStore(bucket, ttlMs)
	if err != nil {
		return err
	}
	_, err = kv.Put(key, []byte(value))
	return err
}

// KvWatch streams bucket entries matching keys (NATS wildcard). Snapshot
// entries arrive first (IsUpdate=false), then live updates; the channel
// closes when the cancel func runs or ctx ends.
func (b *Bus) KvWatch(ctx context.Context, bucket, keys string) (<-chan bus.KvEvent, func(), error) {
	kv, err := b.js.KeyValue(bucket)
	if err != nil {
		return nil, nil, err
	}
	w, err := kv.Watch(keys)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan bus.KvEvent, 32)
	stop := func() { _ = w.Stop() }
	go func() {
		defer close(ch)
		snapshotDone := false
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-w.Updates():
				if !ok {
					return
				}
				if e == nil {
					snapshotDone = true
					select {
					case ch <- bus.KvEvent{Done: true}:
					case <-ctx.Done():
						return
					}
					continue
				}
				ev := bus.KvEvent{
					Key:      e.Key(),
					Revision: e.Revision(),
					IsUpdate: snapshotDone,
				}
				if e.Operation() == nats.KeyValuePut {
					ev.Value = string(e.Value())
				} else {
					ev.Deleted = true
				}
				select {
				case ch <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, stop, nil
}

func (b *Bus) KVGet(ctx context.Context, bucket, key string) (string, error) {
	kv, err := b.js.KeyValue(bucket)
	if err != nil {
		return "", nil
	}
	e, err := kv.Get(key)
	if err != nil {
		// KeyNotFound includes tombstones from a prior Delete — the value is
		// gone either way, so a miss ("" ,nil) is the honest answer.
		return "", nil
	}
	if e.Operation() != nats.KeyValuePut {
		return "", nil
	}
	return string(e.Value()), nil
}

func (b *Bus) KVCas(ctx context.Context, bucket, key, value string, revision int64) (int64, error) {
	kv, err := b.js.KeyValue(bucket)
	if err != nil {
		return 0, nil
	}
	rev, err := kv.Update(key, []byte(value), uint64(revision))
	if err != nil {
		return 0, nil
	}
	return int64(rev), nil
}

func (b *Bus) KVDelete(ctx context.Context, bucket, key string) error {
	kv, err := b.js.KeyValue(bucket)
	if err != nil {
		return err
	}
	return kv.Delete(key)
}

// Replay returns the retained envelopes for a channel (JetStream stream
// contents, oldest first). Backs session-events replay.
func (b *Bus) Replay(ctx context.Context, ch string) ([]abcprotocol.Envelope, error) {
	out := []abcprotocol.Envelope{}
	// Find the stream covering this subject.
	for _, info := range []string{streamEvents, streamMailbox, streamDLQ} {
		si, err := b.js.StreamInfo(info)
		if err != nil {
			continue
		}
		subjects := si.Config.Subjects
		matched := false
		for _, s := range subjects {
			if s == ch || strings.ContainsAny(s, "*>") && subjectMatch(s, ch) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		sub, err := b.js.SubscribeSync(ch, nats.BindStream(info))
		if err != nil {
			return nil, err
		}
		defer sub.Unsubscribe()
		for {
			m, err := sub.NextMsg(500 * time.Millisecond)
			if err != nil {
				break
			}
			env, err := decode(m)
			if err != nil {
				continue
			}
			out = append(out, env)
		}
		break
	}
	return out, nil
}

func (b *Bus) Close() error {
	b.nc.Drain()
	b.nc.Close()
	return nil
}

// subjectMatch implements NATS-style `*` / `>` matching for stream subject
// filters.
func subjectMatch(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	pi := 0
	for si := 0; si < len(s); si++ {
		if pi >= len(p) {
			return false
		}
		pt := p[pi]
		if pt == ">" {
			return true
		}
		if pt == "*" {
			pi++
			continue
		}
		if pt != s[si] {
			return false
		}
		pi++
	}
	return pi == len(p)
}
