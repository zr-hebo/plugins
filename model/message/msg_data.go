package message

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/zr-hebo/plugins/transform"
)

type PackMessage struct {
	// public and export fields
	ChangePos     string   `json:"cp"`
	XID           string   `json:"xid"`
	LastCommitted int64    `json:"lc"`
	BatchNumber   int64    `json:"bn"`
	EventTime     uint64   `json:"et"`
	ReceiveTime   uint64   `json:"rt"`
	Sheets        []*Sheet `json:"ts"`

	// public but not need export fields
	RowCounter int    `json:"-"`
	Size       uint64 `json:"-"`

	// private fields
	contentBytes []byte
}

type ChangeRow struct {
	// before data row image
	BeforeVals *RowData `json:"before,omitempty"`
	// after data row image
	AfterVals *RowData `json:"after,omitempty"`
}

type RowData struct {
	// identity value
	PKVals  []interface{}          `json:"pk_vals,omitempty"`
	Vals    []interface{}          `json:"vals"`
	KVPairs map[string]interface{} `json:"kv_pairs"`
}

// Statement sql statement
type Statement struct {
	// sql statement
	SQL string `json:"sql"`
	// table size, downstream change according to it
	TableSize uint64  `json:"-"`
	DDLType   SQLType `json:"-"`
}

// Sheet means a sql execution or the change rows from it
type Sheet struct {
	DBName      string  `json:"db"`
	TableName   string  `json:"tbl"`
	SQLType     SQLType `json:"st"`
	EventTime   uint64  `json:"et"`
	ReceiveTime uint64  `json:"rt"`

	// sql execution info, maybe DDL or other SQL
	Statement *Statement `json:"sql,omitempty"`
	// change columns and rows from the SQL execution
	Columns []*Column    `json:"cols,omitempty"`
	Rows    []*ChangeRow `json:"rows,omitempty"`

	// some info compute by user
	PKNames []string

	lookupCols map[string]bool
}

func (st *Sheet) AlignNewRecord(record map[string]interface{}, rowData *RowData) {
	for key, val := range record {
		// deal with new generated column
		if !st.lookupCols[key] {
			st.Columns = append(st.Columns, &Column{
				Name:    key,
				RawType: GeneratedColumnType,
			})
			st.lookupCols[key] = true
		}

		rowData.Vals = append(rowData.Vals, val)
	}
	rowData.KVPairs = record
	return
}

func (st *Sheet) ArrangeRows() {
	for _, row := range st.Rows {
		record := make(map[string]interface{}, len(st.Columns))
		if row.BeforeVals != nil {
			for idx := range st.Columns {
				record[st.Columns[idx].Name] = row.BeforeVals.Vals[idx]
			}
			row.BeforeVals.KVPairs = record
		}

		if row.AfterVals != nil {
			record = make(map[string]interface{}, len(st.Columns))
			for idx := range st.Columns {
				record[st.Columns[idx].Name] = row.AfterVals.Vals[idx]
			}
			row.AfterVals.KVPairs = record
		}
	}
	return
}

func (pm *PackMessage) Arrange() {
	for _, sheet := range pm.Sheets {
		sheet.ArrangeRows()
	}
}

func (pm *PackMessage) String() string {
	return string(pm.Bytes())
}

func (pm *PackMessage) Bytes() []byte {
	if pm.contentBytes != nil {
		return pm.contentBytes
	}

	contentBytes, err := json.Marshal(pm)
	if err != nil {
		panic(fmt.Sprintf("Marshal PackMessage:%#v failed <-- %s", pm, err.Error()))
	}

	pm.contentBytes = contentBytes
	return contentBytes
}

func (pm *PackMessage) Transform(tf transform.Transformer) (newMsgs []IMessage, err error) {
	for _, sheet := range pm.Sheets {
		for _, row := range sheet.Rows {
			var newRecord map[string]interface{}
			if row.BeforeVals != nil {
				newRecord, err = tf.Transform(row.BeforeVals.KVPairs)
				if err != nil {
					return
				}

				sheet.AlignNewRecord(newRecord, row.BeforeVals)
			}

			if row.AfterVals != nil {
				newRecord, err = tf.Transform(row.AfterVals.KVPairs)
				if err != nil {
					return
				}

				sheet.AlignNewRecord(newRecord, row.AfterVals)
			}
		}
	}

	newMsgs = []IMessage{pm}
	return
}

func (pm *PackMessage) ToRecordMessages() []*RecordMessage {
	rms := make([]*RecordMessage, 0, 16)
	idx := 0
	for _, sheet := range pm.Sheets {
		if len(sheet.Rows) == 0 {
			idx += 1
			if sheet.Statement == nil {
				continue
			}

			rms = append(rms, &RecordMessage{
				RecordID:    fmt.Sprintf("%s#%d#%d", pm.XID, pm.BatchNumber, idx),
				SQLType:     sheet.SQLType,
				DBName:      sheet.DBName,
				TableName:   sheet.TableName,
				EventTime:   sheet.EventTime,
				ReceiveTime: sheet.ReceiveTime,
				SQL:         sheet.Statement.SQL,
			})

		} else {
			for _, row := range sheet.Rows {
				idx += 1

				rms = append(rms, &RecordMessage{
					RecordID:       fmt.Sprintf("%s#%d#%d", pm.XID, pm.BatchNumber, idx),
					SQLType:        sheet.SQLType,
					DBName:         sheet.DBName,
					TableName:      sheet.TableName,
					EventTime:      sheet.EventTime,
					ReceiveTime:    sheet.ReceiveTime,
					BeforeRowImage: mapKeyValue(sheet.Columns, row.BeforeVals.Vals),
					AfterRowImage:  mapKeyValue(sheet.Columns, row.AfterVals.Vals),
					SQL:            sheet.Statement.SQL,
				})
			}
		}
	}

	return rms
}

func (pm *PackMessage) ToPanamaMessages() []*PanamaMessage {
	pms := make([]*PanamaMessage, 0, 16)
	idx := 0
	for _, sheet := range pm.Sheets {
		if len(sheet.Rows) == 0 {
			idx += 1
			if sheet.Statement == nil {
				continue
			}

			pms = append(pms, &PanamaMessage{
				UUID:          uuid.New().String(),
				ID:            createPanamaEventID(pm.XID, idx),
				SQLType:       sheet.SQLType,
				FullTableName: fmt.Sprintf("%s.%s", sheet.DBName, sheet.TableName),
				EventTime:     pm.EventTime / 1000,
				ReceiveTime:   sheet.ReceiveTime / 1000,
				FieldNum:      uint16(len(sheet.Columns)),
			})

		} else {
			for _, row := range sheet.Rows {
				idx += 1

				pms = append(pms, &PanamaMessage{
					UUID:          uuid.New().String(),
					ID:            createPanamaEventID(pm.XID, idx),
					SQLType:       sheet.SQLType,
					FullTableName: fmt.Sprintf("%s.%s", sheet.DBName, sheet.TableName),
					EventTime:     pm.EventTime / 1000,
					ReceiveTime:   sheet.ReceiveTime / 1000,
					FieldNum:      uint16(len(sheet.Columns)),
					OldRow:        convertMySQLRowToPanama(row.BeforeVals.Vals),
					NewRow:        convertMySQLRowToPanama(row.AfterVals.Vals),
				})
			}
		}
	}

	return pms
}
