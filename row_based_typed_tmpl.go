package main

// {{/*
type _GOTYPE interface{}

func _MULFN(_, _ interface{}) TypedDatum {
	panic("do not call from non-templated code")
}

// */}}

// {{ range .}}
type mulGenerated_TYPEOperator struct {
	input             TypedOperator
	arg               _GOTYPE
	columnsToMultiply []int
}

func (m mulGenerated_TYPEOperator) next() []TypedDatum {
	row := m.input.next()
	if row == nil {
		return nil
	}
	for _, c := range m.columnsToMultiply {
		row[c] = _MULFN(row[c], m.arg)
	}
	return row
}

// {{ end }}
