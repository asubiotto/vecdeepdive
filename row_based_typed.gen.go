// This is generated code. DO NOT EDIT.
package main

// 

// 
type mulInt64Operator struct {
	input             TypedOperator
	arg               int64
	columnsToMultiply []int
}

func (m mulInt64Operator) next() []TypedDatum {
	row := m.input.next()
	if row == nil {
		return nil
	}
	for _, c := range m.columnsToMultiply {
		row[c] = TypedDatum{t: Int64Type, int64: row[c].int64* m.arg}
	}
	return row
}

// 
type mulFloat64Operator struct {
	input             TypedOperator
	arg               float64
	columnsToMultiply []int
}

func (m mulFloat64Operator) next() []TypedDatum {
	row := m.input.next()
	if row == nil {
		return nil
	}
	for _, c := range m.columnsToMultiply {
		row[c] = TypedDatum{t: Float64Type, float64: row[c].float64* m.arg}
	}
	return row
}

// 
