package bolt

import (
	"bytes"
	"encoding/json"
	"errors"
)

const formatVersion = 1

// ErrUnsupportedFormat reports a stored value whose format version is unknown.
var ErrUnsupportedFormat = errors.New("durable jobs bolt: unsupported format")

type envelope struct {
	Version int             `json:"version"`
	Payload json.RawMessage `json:"payload"`
}

func encode(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{Version: formatVersion, Payload: payload})
}

func decode(data []byte, value any) error {
	var stored envelope
	if err := json.Unmarshal(bytes.Clone(data), &stored); err != nil {
		return err
	}
	if stored.Version != formatVersion {
		return ErrUnsupportedFormat
	}
	return json.Unmarshal(stored.Payload, value)
}
