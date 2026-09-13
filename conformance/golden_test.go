package conformance

import (
	"testing"

	"github.com/abcp-sdk/abc-protocol-go/protocol"
)

// goldenVectors mirrors tests/golden.ts in @abc-protocol/sdk. If either side
// changes a derivation, the other must be updated in lockstep. v2: every
// data-plane channel is abc.<tenant>.<...>.
const goldenTenant = "alice"

var goldenVectors = map[string]struct{ got, want string }{
	"session_token":     {protocol.SessionToken("sess-1"), "q-Yz86R6J1gXTqvpFg2vNs"},
	"discover":          {protocol.ChDiscover, "abc.discover"},
	"tool_call":         {protocol.ChToolCall(goldenTenant, "ops", "echo"), "abc.alice.tool.call.ops.echo"},
	"tool_progress":     {protocol.ChToolProgress(goldenTenant, "c1"), "abc.alice.tool.progress.c1"},
	"variable":          {protocol.ChVariable(goldenTenant, "ops", "base-url"), "abc.alice.var.ops.base-url"},
	"interrupt":         {protocol.ChInterrupt(goldenTenant, "ops"), "abc.alice.ctl.interrupt.ops"},
	"hook_call":         {protocol.ChHookCall(goldenTenant, "ops", "session.before_create"), "abc.alice.hook.call.ops.session.before_create"},
	"hook_event":        {protocol.ChHookEvent(goldenTenant, "session.created"), "abc.alice.hook.event.session.created"},
	"mailbox_wildcard":  {protocol.MailboxWildcardAll + ">", "abc.*.mailbox.>"},
	"mailbox_sess1":     {protocol.ChMailbox(goldenTenant, "sess-1"), "abc.alice.mailbox.q-Yz86R6J1gXTqvpFg2vNs"},
	"session_events_s1": {protocol.ChSessionEvents(goldenTenant, "sess-1"), "abc.alice.session.events.q-Yz86R6J1gXTqvpFg2vNs"},
	"config":            {protocol.ChConfig(goldenTenant, "ops"), "abc.alice.config.ops"},
	"config_get":        {protocol.ChConfigGet(goldenTenant, "ops"), "abc.alice.config.get.ops"},
	"var_key":           {protocol.VarKey(goldenTenant, "ops", "base-url"), "t.alice.ops.base-url"},
	"session_var_key":   {protocol.SessionVarKey(goldenTenant, "ops", "sess-1", "ws"), "t.alice.ops.q-Yz86R6J1gXTqvpFg2vNs.ws"},
}

func TestGoldenVectors(t *testing.T) {
	for name, v := range goldenVectors {
		if v.got != v.want {
			t.Errorf("%s = %q, want %q (TS/Go derivations drifted)", name, v.got, v.want)
		}
	}
}
