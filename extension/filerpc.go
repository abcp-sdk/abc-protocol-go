package extension

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/abcp-sdk/abc-protocol-go/bus"
	"github.com/abcp-sdk/abc-protocol-go/protocol"
)

// IngestFileViaAgent stores bytes through the AGENT (a 1:1 `req` on
// `abc.<tenant>.file.ingest`) and returns the canonical `file:<code>`.
//
// For extensions that own no blob backend or metadata store (no S3
// credentials, no agent DB): the agent persists the bytes to the configured
// object store and the metadata to the configured meta backend, using the SAME
// path as an in-process ingest. The tenant rides the subject/envelope, so no
// separate credential is introduced.
func IngestFileViaAgent(
	ctx context.Context,
	b bus.Bus,
	tenant, name, mime string,
	data []byte,
	sessionName string,
) (string, error) {
	req := protocol.FileIngestRequest{
		Name:        name,
		Mime:        mime,
		Data:        base64.StdEncoding.EncodeToString(data),
		SessionName: sessionName,
	}
	opts := bus.RequestOpts{Tenant: tenant}
	if sessionName != "" {
		opts.SessionName = sessionName
	}
	env, err := b.Request(ctx, protocol.ChFileIngest(tenant), req, opts)
	if err != nil {
		return "", fmt.Errorf("file ingest request: %w", err)
	}
	var res protocol.FileIngestResponse
	if !protocol.Coerce(env.Payload, &res) {
		return "", fmt.Errorf("file ingest: malformed response")
	}
	if !res.Ok {
		if res.Error != nil && res.Error.Message != "" {
			return "", fmt.Errorf("file ingest: %s", res.Error.Message)
		}
		return "", fmt.Errorf("file ingest failed")
	}
	return res.Code, nil
}

// GetFileViaAgent fetches a stored file's bytes + metadata through the agent
// (a 1:1 `req` on `abc.<tenant>.file.get`). Returns (nil, nil, nil) when the
// file is absent.
func GetFileViaAgent(
	ctx context.Context,
	b bus.Bus,
	tenant, code string,
) (*protocol.FileMetaWire, []byte, error) {
	env, err := b.Request(
		ctx,
		protocol.ChFileGet(tenant),
		protocol.FileGetRequest{Code: code},
		bus.RequestOpts{Tenant: tenant},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("file get request: %w", err)
	}
	var res protocol.FileGetResponse
	if !protocol.Coerce(env.Payload, &res) {
		return nil, nil, fmt.Errorf("file get: malformed response")
	}
	if !res.Ok || res.Meta == nil || res.Data == "" {
		return nil, nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(res.Data)
	if err != nil {
		return nil, nil, fmt.Errorf("file get: bad base64: %w", err)
	}
	return res.Meta, data, nil
}
