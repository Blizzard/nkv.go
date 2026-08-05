package nkv

import "encoding/json"

// Codec defines how values are marshaled to/from the wire []byte
// representation stored in the KV bucket. Each codec has a MIME content
// type that is stored in the Content-Type message header.
type Codec struct {
	// ContentType is the MIME type written to the Content-Type header
	// (e.g. "application/json", "application/protobuf").
	ContentType string
	Marshal     func(any) ([]byte, error)
	Unmarshal   func([]byte, any) error
}

// JSONCodec returns a Codec that uses encoding/json with content type
// "application/json".
func JSONCodec() Codec {
	return Codec{
		ContentType: contentTypeJSON,
		Marshal:     json.Marshal,
		Unmarshal:   json.Unmarshal,
	}
}

// Well-known MIME content types.
const (
	contentTypeJSON = "application/json"
)
