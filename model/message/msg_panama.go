package message

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

// PanamaMessage Event describes GDS event
type PanamaMessage struct {
	UUID          string                 `json:"uuid"`            // message UUID
	ID            string                 `json:"source"`          // event source
	SQLType       SQLType                `json:"command"`         // mysql command
	FullTableName string                 `json:"table"`           // mysql table name
	EventTime     uint64                 `json:"timestamp"`       // mysql commit timestamp
	ReceiveTime   uint64                 `json:"timestamp_gds"`   // gds commit timestamp (at replication consumption, before any processing)
	FieldNum      uint16                 `json:"fieldnum"`        // number of columns
	NewRow        map[string]*string     `json:"newrow"`          // old DB row values
	OldRow        map[string]*string     `json:"oldrow"`          // new DB row values
	ExtraInfo     map[string]interface{} `json:"extra,omitempty"` // put misc information like source

	// private fields
	contentBytes []byte
}

func (pm *PanamaMessage) String() string {
	return string(pm.Bytes())
}

func (pm *PanamaMessage) Bytes() []byte {
	if pm.contentBytes != nil {
		return pm.contentBytes
	}

	contentBytes, err := json.Marshal(pm)
	if err != nil {
		panic(fmt.Sprintf("Marshal Panama Message:%#v failed <-- %s", pm, err.Error()))
	}

	pm.contentBytes = contentBytes
	return contentBytes
}

// convertMySQLRowToPanama parse binlog DB row to map of strings
func convertMySQLRowToPanama(row []interface{}) (rowData map[string]*string) {
	if row == nil {
		return nil
	}

	rowData = map[string]*string{}

	for i, data := range row {
		var tmpVal string
		if data == nil {
			rowData[strconv.Itoa(i)] = nil
			continue
		}

		typeFound := true
		switch data.(type) {
		case int:
			tmpVal = strconv.FormatInt(int64(data.(int)), 10)
		case int8:
			tmpVal = strconv.FormatInt(int64(data.(int8)), 10)
		case int16:
			tmpVal = strconv.FormatInt(int64(data.(int16)), 10)
		case int32:
			tmpVal = strconv.FormatInt(int64(data.(int32)), 10)
		case int64:
			tmpVal = strconv.FormatInt(data.(int64), 10)

		case uint:
			tmpVal = strconv.FormatUint(uint64(data.(uint)), 10)
		case uint8:
			tmpVal = strconv.FormatUint(uint64(data.(uint8)), 10)
		case uint16:
			tmpVal = strconv.FormatUint(uint64(data.(uint16)), 10)
		case uint32:
			tmpVal = strconv.FormatUint(uint64(data.(uint32)), 10)
		case uint64:
			tmpVal = strconv.FormatUint(data.(uint64), 10)

		case float32:
			tmpVal = strconv.FormatFloat(float64(data.(float32)), 'f', -1, 32)
		case float64:
			tmpVal = strconv.FormatFloat(data.(float64), 'f', -1, 64)

		case bool:
			tmpVal = strconv.FormatBool(data.(bool))

		case decimal.Decimal:
			tmpVal = data.(decimal.Decimal).String()

		case []byte:
			tmpVal = hex.EncodeToString(data.([]byte))

		case string:
			tmpVal = hex.EncodeToString([]byte(data.(string)))

		default:
			typeFound = false
		}

		if typeFound {
			rowData[strconv.Itoa(i)] = &tmpVal
		} else {
			rowData[strconv.Itoa(i)] = nil
		}
	}
	return
}

func createPanamaEventID(xid string, evtIdx int) string {
	return fmt.Sprintf("%s.%d", strings.Replace(xid, ":", ".", -1), evtIdx)
}
