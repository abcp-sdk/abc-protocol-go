package conformance

import (
	"testing"

	"forgejo.develop.10.199.64.20.nip.io/abc-protocol/sdk-go/protocol"
)

// goldenVectors mirrors packages/sdk/tests/golden.ts in sdk-ts. If either
// side changes a derivation, the other must be updated in lockstep.
var goldenVectors = map[string]struct{ got, want string }{
	"session_token":     {protocol.SessionToken("sess-1"), "q-Yz86R6J1gXTqvpFg2vNs"},
	"discover":          {protocol.ChDiscover, "abc.discover"},
	"tool_call":         {protocol.ChToolCall("ops", "echo"), "abc.tool.call.ops.echo"},
	"tool_progress":     {protocol.ChToolProgress("c1"), "abc.tool.progress.c1"},
	"variable":          {protocol.ChVariable("ops", "base-url"), "abc.var.ops.base-url"},
	"interrupt":         {protocol.ChInterrupt("ops"), "abc.ctl.interrupt.ops"},
	"hook_call":         {protocol.ChHookCall("ops", "session.before_create"), "abc.hook.call.ops.session.before_create"},
	"hook_event":        {protocol.ChHookEvent("session.created"), "abc.hook.event.session.created"},
	"mailbox_wildcard":  {protocol.MailboxWildcard + ">", "abc.mailbox.>"},
	"mailbox_sess1":     {protocol.ChMailbox("sess-1"), "abc.mailbox.q-Yz86R6J1gXTqvpFg2vNs"},
	"session_events_s1": {protocol.ChSessionEvents("sess-1"), "abc.session.events.q-Yz86R6J1gXTqvpFg2vNs"},
	"config":            {protocol.ChConfig("ops"), "abc.config.ops"},
	"config_get":        {protocol.ChConfigGet("ops"), "abc.config.get.ops"},
}

func TestGoldenVectors(t *testing.T) {
	for name, v := range goldenVectors {
		if v.got != v.want {
			t.Errorf("%s = %q, want %q (TS/Go derivations drifted)", name, v.got, v.want)
		}
	}
}
