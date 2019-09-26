package main

import (
	"fmt"
)

const batchSize = 1024

type vector interface {
	// Type returns the type of data stored in this vector.
	Type() T
	// Int64 returns an int64 slice.
	Int64() []int64
	// Float64 returns a float64 slice.
	Float64() []float64
	// Slice returns a new vector sliced to the given indices.
	Slice(colType T, start, end int) vector
	// SetCol sets the vector to have the given data.
	SetCol(interface{})
}

type column struct {
	t T
	col interface{}
}

// newColumn returns a new column, initialized with a length.
func newColumn(t T, n int) vector {
	switch t {
	case Int64Type:
		return &column{t: t, col: make([]int64, n)}
	case Float64Type:
		return &column{t: t, col: make([]float64, n)}
	default:
		panic(fmt.Sprintf("unhandled type",))
	}
}

func (c *column) Type() T {
	return c.t
}

func (c *column) Int64() []int64 {
	return c.col.([]int64)
}

func (c *column) Float64() []float64 {
	return c.col.([]float64)
}

func (c *column) Slice(colType T, start, end int) vector {
	switch colType {
	case Int64Type:
		col := c.Int64()
		return &column{
			t:   colType,
			col: col[start:end],
		}
	case Float64Type:
		col := c.Float64()
		return &column{
			t:   colType,
			col: col[start:end],
		}
	default:
		panic("unhandled type")
	}
}

func (c *column) SetCol(col interface{}) {
	c.col = col
}

type colBatch struct {
	size int
	vecs []vector
}

type TypedColOperator interface {
	next() colBatch
}

type mulInt64ColOperator struct {
	input             TypedColOperator
	arg               int64
	columnsToMultiply []int
}

func (m mulInt64ColOperator) next() colBatch {
	batch := m.input.next()
	if batch.size == 0 {
		return batch
	}
	for _, c := range m.columnsToMultiply {
		vec := batch.vecs[c].Int64()
		for i := range vec {
			vec[i] = vec[i] * m.arg
		}
	}
	return batch
}

type mulFloat64ColOperator struct {
	input             TypedColOperator
	arg               float64
	columnsToMultiply []int
}

func (m mulFloat64ColOperator) next() colBatch {
	batch := m.input.next()
	if batch.size == 0 {
		return batch
	}
	for _, c := range m.columnsToMultiply {
		vec := batch.vecs[c].Float64()
		for i := range vec {
			vec[i] = vec[i] * m.arg
		}
	}
	return batch
}

// typedColTableReader takes unlimited-size columns and chunks them into
// the batchSize when Next is called.
type typedColTableReader struct {
	curIdx int
	length int
	batch colBatch
}

func (t *typedColTableReader) next() colBatch {
	if t.curIdx >= t.length {
		t.batch.size = 0
		return t.batch
	}
	endIdx := t.curIdx + batchSize
	if endIdx > t.length {
		endIdx = t.length
	}
	for i, col := range t.batch.vecs {
		t.batch.vecs[i] = col.Slice(col.Type(), t.curIdx, endIdx)
	}
	t.batch.size = endIdx - t.curIdx
	t.curIdx = endIdx
	return t.batch
}

func (t *typedColTableReader) reset() {
	t.curIdx = 0
}

// makeTypedColInput creates numRows rows of numCols each of the given type. For
// each row, all of its columns will be its index (zero-indexed).
func makeTypedColInput(numRows int, numCols int, t T) colBatch {
	result := make([]vector, numCols)
	for i := range result {
		result[i] = newColumn(t, numRows)
	}
	switch t {
	case Int64Type:
		for i := 0; i < numCols; i++ {
			col := result[i].Int64()
			for j := 0; j < numRows; j++ {
				col[j] = int64(i)
			}
		}
	case Float64Type:
		for i := 0; i < numCols; i++ {
			col := result[i].Float64()
			for j := 0; j < numRows; j++ {
				col[j] = float64(i)
			}
		}
	default:
		panic("unhandled type")
	}
	return colBatch{size:numRows, vecs:result}
}
