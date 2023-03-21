package message



// DataMessage 一个Message对应一个事务
type DataMessage struct {
	Region        string `json:"rg"`
	XID           string `json:"xid"`
	ChangePos     string `json:"cp"`
	LastCommitted int64  `json:"lc"`
	BatchNumber   int64  `json:"bn"`
	EventTime     int64  `json:"et"`

	RowCounter   int          `json:"-"`
	ReceiveTime  int64        `json:"-"`

	// data message size only calculate once
	Size uint64 `json:"-"`
}

