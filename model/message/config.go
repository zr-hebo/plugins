package message

const (
	GeneratedColumnType = "plugin-generated"
)

var (
	NullInBytes = []byte("NULL")
)

type SQLType uint8

const (
	SQLTypeNull SQLType = iota
	SQLTypeInsert
	SQLTypeUpdate
	SQLTypeDelete
	SQLTypeDDL
	SQLTypeBatch
	SQLTypeInitSchema
	SQLTypeOther
)

func (st *SQLType) String() string {
	switch *st {
	case SQLTypeInsert:
		return "insert"
	case SQLTypeUpdate:
		return "update"
	case SQLTypeDelete:
		return "delete"
	case SQLTypeDDL:
		return "ddl"
	case SQLTypeBatch:
		return "batch"
	case SQLTypeInitSchema:
		return "init_schema"
	case SQLTypeNull:
		return "null"
	default:
		return "other"
	}
}
