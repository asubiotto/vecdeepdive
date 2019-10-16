package main

type Operator interface {
	next() []Datum
}

type Datum interface{}

type mulFn func(a Datum, b Datum) Datum

func mulIntDatums(a Datum, b Datum) Datum {
	aInt := a.(Int).int64
	bInt := b.(Int).int64
	return Int{int64: aInt * bInt}
}

func mulFloat64Datums(a Datum, b Datum) Datum {
	aFloat := a.(Float64).float64
	bFloat := a.(Float64).float64
	return Float64{float64: aFloat * bFloat}
}

// Int implements the Datum interface.
type Int struct {
	int64
}

// Float64 implements the Datum interface.
type Float64 struct {
	float64
}

type mulOperator struct {
	input             Operator
	fn                mulFn
	arg               Datum
	columnsToMultiply []int
}

func (m mulOperator) next() []Datum {
	row := m.input.next()
	if row == nil {
		return nil
	}
	for _, c := range m.columnsToMultiply {
		row[c] = m.fn(row[c], m.arg)
	}
	return row
}

type tableReader struct {
	curIdx int
	rows   [][]Datum
}

func (t *tableReader) next() []Datum {
	if t.curIdx >= len(t.rows) {
		return nil
	}
	row := t.rows[t.curIdx]
	t.curIdx++
	return row
}

func (t *tableReader) reset() {
	t.curIdx = 0
}

// makeInput creates numRows rows of numCols each of the given type. For each
// row, all of its columns will be its index (zero-indexed).
func makeInput(numRows int, numCols int, t Datum) [][]Datum {
	result := make([][]Datum, numRows)
	for i := range result {
		result[i] = make([]Datum, numCols)
	}
	switch t.(type) {
	case Int:
		for i := 0; i < numRows; i++ {
			for j := 0; j < numCols; j++ {
				result[i][j] = Int{int64: int64(i)}
			}
		}
	case Float64:
		for i := 0; i < numRows; i++ {
			for j := 0; j < numCols; j++ {
				result[i][j] = Float64{float64: float64(i)}
			}
		}
	default:
		panic("unhandled type")
	}
	return result
}
