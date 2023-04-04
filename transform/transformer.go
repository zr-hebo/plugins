package transform

type Transformer interface {
	Transform(rawVals map[string]interface{}) (newVals map[string]interface{}, err error)
}

type tfConst struct {
	ColName string
	Val     interface{}
}

func NewTFConst(colName string, val interface{}) Transformer {
	return &tfConst{
		ColName: colName,
		Val:     val,
	}
}

// Transform in tfConst transform the target value to a given const value
func (tc *tfConst) Transform(rawVals map[string]interface{}) (newVals map[string]interface{}, err error) {
	if len(rawVals) == 0 {
		return
	}

	newVals = make(map[string]interface{}, len(rawVals))
	for colName := range rawVals {
		if colName == tc.ColName {
			newVals[colName] = tc.Val
		}
	}
	return
}
