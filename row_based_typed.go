package main

type T int

const (
	// Int64Type is a value of type int64
	Int64Type T = iota
	// Float64Type is a value of type float64
	Float64Type
)

type TypedDatum struct {
	t       T
	int64   int64
	float64 float64
}

type TypedOperator interface {
	next() []TypedDatum
}

type typedTableReader struct {
	curIdx int
	rows   [][]TypedDatum
}

func (t *typedTableReader) next() []TypedDatum {
	if t.curIdx >= len(t.rows) {
		return nil
	}
	row := t.rows[t.curIdx]
	t.curIdx++
	return row
}

func (t *typedTableReader) reset() {
	t.curIdx = 0
}

// makeTypedInput creates numRows rows of numCols each of the given type. For
// each  row, all of its columns will be its index (zero-indexed).
func makeTypedInput(numRows int, numCols int, t T) [][]TypedDatum {
	result := make([][]TypedDatum, numRows)
	for i := range result {
		result[i] = make([]TypedDatum, numCols)
	}
	switch t {
	case Int64Type:
		for i := 0; i < numRows; i++ {
			for j := 0; j < numCols; j++ {
				result[i][j] = TypedDatum{t: t, int64: int64(i)}
			}
		}
	case Float64Type:
		for i := 0; i < numRows; i++ {
			for j := 0; j < numCols; j++ {
				result[i][j] = TypedDatum{t: t, float64: float64(i)}
			}
		}
	default:
		panic("unhandled type")
	}
	return result
}
