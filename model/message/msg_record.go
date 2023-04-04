package message

import (
	"encoding/json"
	"fmt"

	"github.com/zr-hebo/plugins/transform"
)

// RecordMessage Event describes GDS event
type RecordMessage struct {
	RecordID       string                 `json:"id"` // message UUID
	SQLType        SQLType                `json:"st"`
	DBName         string                 `json:"db"`              // mysql table name
	TableName      string                 `json:"tbl_name"`        // mysql table name
	EventTime      uint64                 `json:"et"`              // mysql timestamp in binlog
	ReceiveTime    uint64                 `json:"rt"`              // DTS receive binlog timestamp
	SQL            string                 `json:"sql,omitempty"`   // SQL statement
	BeforeRowImage map[string]interface{} `json:"old,omitempty"`   // new DB row values
	AfterRowImage  map[string]interface{} `json:"new,omitempty"`   // old DB row values
	ExtraInfo      map[string]interface{} `json:"extra,omitempty"` // put misc information like source

	// private fields
	contentBytes []byte
}

func (rm *RecordMessage) String() string {
	return string(rm.Bytes())
}

func (rm *RecordMessage) Bytes() []byte {
	if rm.contentBytes != nil {
		return rm.contentBytes
	}

	contentBytes, err := json.Marshal(rm)
	if err != nil {
		panic(fmt.Sprintf("Marshal Record Message:%#v failed <-- %s", rm, err.Error()))
	}

	rm.contentBytes = contentBytes
	return contentBytes
}

func (rm *RecordMessage) Transform(tf transform.Transformer) (newMsgs []IMessage, err error) {
	newVals, err := tf.Transform(rm.BeforeRowImage)
	if err != nil {
		return
	}
	rm.BeforeRowImage = newVals

	newVals, err = tf.Transform(rm.AfterRowImage)
	if err != nil {
		return
	}
	rm.AfterRowImage = newVals
	newMsgs = []IMessage{rm}
	return
}

func mapKeyValue(cols []*Column, row []interface{}) (kv map[string]interface{}) {
	kv = make(map[string]interface{}, len(cols))
	for idx, col := range cols {
		val := row[idx]
		kv[col.Name] = val
	}
	return kv
}
