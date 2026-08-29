package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// SessionToken derives the transport-safe token for a session name.
func SessionToken(sessionName string) string {
	sum := sha256.Sum256([]byte(sessionName))
	return base64.RawURLEncoding.EncodeToString(sum[:])[:22]
}

const (
	ChDiscover        = "abc.discover"
	MailboxWildcard   = "abc.mailbox."
	LifecycleWildcard = "abc.session.lifecycle."
	// VarsBucket stores extension variables: global vars are keyed
	// vars.<extId>.<name>, session vars vars.<extId>.<sessionToken>.<name>.
	VarsBucket = "vars"
)

func ChToolCall(extID, tool string) string  { return "abc.tool.call." + extID + "." + tool }
func ChToolProgress(callID string) string   { return "abc.tool.progress." + callID }
func ChVariable(extID, name string) string  { return "abc.var." + extID + "." + name }
func ChMailbox(session string) string       { return "abc.mailbox." + SessionToken(session) }
func ChSessionEvents(session string) string { return "abc.session.events." + SessionToken(session) }
func ChInterrupt(extID string) string       { return "abc.ctl.interrupt." + extID }
func ChHookCall(extID, hook string) string  { return "abc.hook.call." + extID + "." + hook }
func ChHookEvent(hook string) string        { return "abc.hook.event." + hook }

// ChLifecycle routes one lifecycle kind (created/forked/renamed/deleted).
func ChLifecycle(kind string) string { return LifecycleWildcard + kind }

// VarKey is the global variable KV key (vars.<extId>.<name>).
func VarKey(extID, name string) string { return extID + "." + name }

// SessionVarKey is the session variable KV key (vars.<extId>.<token>.<name>).
func SessionVarKey(extID, sessionName, name string) string {
	return extID + "." + SessionToken(sessionName) + "." + name
}

// ArgString reads a string tool argument (missing/typed wrong = "").
func ArgString(args map[string]any, key string) string {
	if v, ok := args[key].(string); ok {
		return v
	}
	return ""
}

// ArgInt reads a numeric tool argument with a default (JSON numbers decode
// as float64; integral values only).
func ArgInt(args map[string]any, key string, def int64) int64 {
	if v, ok := args[key].(float64); ok {
		return int64(v)
	}
	return def
}

// ArgFloat reads a float tool argument with a default.
func ArgFloat(args map[string]any, key string, def float64) float64 {
	if v, ok := args[key].(float64); ok {
		return v
	}
	return def
}

// ArgBool reads a boolean tool argument with a default.
func ArgBool(args map[string]any, key string, def bool) bool {
	if v, ok := args[key].(bool); ok {
		return v
	}
	return def
}
func ChConfig(extID string) string { return "abc.config." + extID }

// ChConfigGet is DEPRECATED (0.2): config snapshots moved to the cfg KV
// bucket; extensions recover via KV watch. Kept for external callers.
func ChConfigGet(extID string) string { return "abc.config.get." + extID }
func ChConfigWildcard() string        { return "abc.config.get.>" }

// ConfigExtIDFromGetChannel extracts the extId from abc.config.get.<extId>.
func ConfigExtIDFromGetChannel(ch string) string {
	return strings.TrimPrefix(ch, "abc.config.get.")
}

// ConfigKVBucket mirrors applied config values for persistence (caps.kv).
const ConfigKVBucket = "cfg"

// NewID returns a fresh random UUID-like id.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "id"
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[0:4]) + "-" + hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" + hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// Coerce round-trips an arbitrary decoded value into a typed shape via JSON.
func Coerce(src any, dst any) bool {
	b, err := json.Marshal(src)
	if err != nil {
		return false
	}
	return json.Unmarshal(b, dst) == nil
}

// Ptr returns a pointer to v. Useful for optional (pointer) fields in
// generated types: Envelope{V: protocol.Ptr(1)}.
func Ptr[T any](v T) *T { return &v }
