package main

// {{/*
type _GOTYPE interface{}

// _MULFN assigns the result of the multiplication of the first and second
// operand to the first operand.
func _MULFN(_ TypedDatum, _ interface{}) {
	panic("do not call from non-templated code")
}

// */}}

// {{ range .}}
type mul_TYPEOperator struct {
	input             TypedOperator
	arg               _GOTYPE
	columnsToMultiply []int
}

func (m mul_TYPEOperator) next() []TypedDatum {
	row := m.input.next()
	if row == nil {
		return nil
	}
	for _, c := range m.columnsToMultiply {
		_MULFN(row[c], m.arg)
	}
	return row
}

// {{ end }}
