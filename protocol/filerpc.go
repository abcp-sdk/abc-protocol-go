package protocol

// File RPCs (agent-served). A DB-less, store-less extension (no S3
// credentials, no agent DB) cannot mint a canonical `file:<code>` on its own,
// so it asks the AGENT to persist the bytes + metadata via the SAME path as an
// in-process ingest. The tenant rides the subject (`abc.<tenant>.file.ingest`)
// and the envelope, so no separate credential is introduced.
const (
	// FileIngestWildcard is the cross-tenant consume wildcard for file.ingest.
	FileIngestWildcard = "abc.*.file.ingest"
	// FileGetWildcard is the cross-tenant consume wildcard for file.get.
	FileGetWildcard = "abc.*.file.get"
)

// ChFileIngest routes a file-ingest request for one tenant.
func ChFileIngest(tenant string) string {
	return TenantPrefix(tenant) + "file.ingest"
}

// ChFileGet routes a file-get request for one tenant.
func ChFileGet(tenant string) string {
	return TenantPrefix(tenant) + "file.get"
}

// FileIngestRequest asks the agent to store bytes and return a file code.
// `Data` is base64 on the wire.
type FileIngestRequest struct {
	Code        string `json:"code,omitempty"`
	Name        string `json:"name"`
	Mime        string `json:"mime"`
	Data        string `json:"data"`
	SessionName string `json:"session_name,omitempty"`
}

// FileMetaWire mirrors the agent's file metadata record on the wire.
type FileMetaWire struct {
	Code            string `json:"code"`
	Sha256          string `json:"sha256"`
	Name            string `json:"name"`
	Mime            string `json:"mime"`
	Size            int64  `json:"size"`
	UploaderSession string `json:"uploader_session,omitempty"`
	CreatedAt       string `json:"created_at"`
}

// FileErrorPayload carries a standard error code + message.
type FileErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// FileIngestResponse is the reply to a FileIngestRequest.
type FileIngestResponse struct {
	Ok    bool              `json:"ok"`
	Code  string            `json:"code,omitempty"`
	Error *FileErrorPayload `json:"error,omitempty"`
}

// FileGetRequest asks the agent for a stored file's bytes + metadata.
type FileGetRequest struct {
	Code string `json:"code"`
}

// FileGetResponse is the reply to a FileGetRequest. `Data` is base64.
type FileGetResponse struct {
	Ok    bool              `json:"ok"`
	Meta  *FileMetaWire     `json:"meta,omitempty"`
	Data  string            `json:"data,omitempty"`
	Error *FileErrorPayload `json:"error,omitempty"`
}
