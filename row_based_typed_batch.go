package main

const batchSize = 1024

type TypedBatchOperator interface {
	next() [][]TypedDatum
}

type mulInt64BatchOperator struct {
	input             TypedBatchOperator
	arg               int64
	columnsToMultiply []int
}

func (m mulInt64BatchOperator) next() [][]TypedDatum {
	rows := m.input.next()
	if rows == nil {
		return nil
	}
	for _, row := range rows {
		for _, c := range m.columnsToMultiply {
			row[c] = TypedDatum{t: Int64Type, int64: row[c].int64 * m.arg}
		}
	}
	return rows
}

type mulFloat64BatchOperator struct {
	input             TypedBatchOperator
	arg               float64
	columnsToMultiply []int
}

func (m mulFloat64BatchOperator) next() [][]TypedDatum {
	rows := m.input.next()
	if rows == nil {
		return nil
	}
	for _, row := range rows {
		for _, c := range m.columnsToMultiply {
			row[c] = TypedDatum{t: Float64Type, float64: row[c].float64 * m.arg}
		}
	}
	return rows
}

type typedBatchTableReader struct {
	curIdx int
	rows   [][]TypedDatum
}

func (t *typedBatchTableReader) next() [][]TypedDatum {
	if t.curIdx >= len(t.rows) {
		return nil
	}
	endIdx := t.curIdx + batchSize
	if endIdx > len(t.rows) {
		endIdx = len(t.rows)
	}
	retRows := t.rows[t.curIdx:endIdx]
	t.curIdx = endIdx
	return retRows
}

func (t *typedBatchTableReader) reset() {
	t.curIdx = 0
}

// makeTypedInput creates numRows rows of numCols each of the given type. For
// each  row, all of its columns will be its index (zero-indexed).
func makeTypedBatchInput(numRows int, numCols int, t T) [][]TypedDatum {
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
