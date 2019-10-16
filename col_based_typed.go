package main

import (
	"fmt"
)

type vector interface {
	// Type returns the type of data stored in this vector.
	Type() T
	// Int64 returns an int64 slice.
	Int64() []int64
	// Float64 returns a float64 slice.
	Float64() []float64
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
	batches []colBatch
}

func (t *typedColTableReader) next() colBatch {
	if t.curIdx >= len(t.batches) {
		return colBatch{size:0}
	}
	batch := t.batches[t.curIdx]
	t.curIdx++
	return batch
}

func (t *typedColTableReader) reset() {
	t.curIdx = 0
}

// makeTypedColInput creates numRows rows of numCols each of the given type,
// divided up into separate batches.
func makeTypedColInput(numRows int, numCols int, t T) typedColTableReader {
	batches := make([]colBatch, 0)
	curIdx := 0
	for curIdx < numRows {
		thisBatchSize := batchSize
		if curIdx + thisBatchSize > numRows {
			thisBatchSize = numRows - curIdx
		}

		vecs := make([]vector, numCols)
		for  i := range vecs {
			vecs[i] = newColumn(t, thisBatchSize)
			switch t {
			case Int64Type:
				for j := 0; j < thisBatchSize; j++ {
					vecs[i].Int64()[j] = int64(j)
				}
			case Float64Type:
				for j := 0; j < thisBatchSize; j++ {
					vecs[i].Float64()[j] = float64(j)
				}
			default:
				panic("unhandled type")
			}
		}

		batches = append(batches, colBatch{size:thisBatchSize, vecs:vecs})
		curIdx += thisBatchSize
	}

	return typedColTableReader{batches: batches}
}
