package message

import "github.com/zr-hebo/plugins/transform"

type IMessage interface {
	String() string
	Bytes() []byte
	Transform(tf transform.Transformer) (newMsgs []IMessage, err error)
}

// Column column info
type Column struct {
	Name     string `json:"name"`
	RawType  string `json:"type"`
	Nullable bool   `json:"nullable"`
}
