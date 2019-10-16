package main

import "testing"

const (
	numRows = 65536
	numCols = 1
)

func BenchmarkRowBasedInterface(b *testing.B) {
	scan := &tableReader{rows: makeInput(numRows, numCols, Int{})}
	render := mulOperator{
		input:             scan,
		fn:                mulIntDatums,
		arg:               Int{2},
		columnsToMultiply: []int{0},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			row := render.next()
			if row == nil {
				break
			}
		}
		scan.reset()
	}
}

func BenchmarkRowBasedTyped(b *testing.B) {
	scan := &typedTableReader{rows: makeTypedInput(numRows, numCols, Int64Type)}
	render := mulInt64Operator{
		input:             scan,
		arg:               2,
		columnsToMultiply: []int{0},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			row := render.next()
			if row == nil {
				break
			}
		}
		scan.reset()
	}
}

func BenchmarkRowBasedTypedBatch(b *testing.B) {
	scan := &typedBatchTableReader{rows: makeTypedBatchInput(numRows, numCols, Int64Type)}
	render := mulInt64BatchOperator{
		input:             scan,
		arg:               2,
		columnsToMultiply: []int{0},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			row := render.next()
			if row == nil {
				break
			}
		}
		scan.reset()
	}
}

func BenchmarkColBasedTyped(b *testing.B) {
	scan := makeTypedColInput(numRows, numCols, Int64Type)
	render := mulInt64ColOperator{
		input:             &scan,
		arg:               2,
		columnsToMultiply: []int{0},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			row := render.next()
			if row.size == 0 {
				break
			}
		}
		scan.reset()
	}
}

func mulInt(a, b Int) Int {
	return Int{int64: a.int64 * b.int64}
}

func BenchmarkSpeedOfLight(b *testing.B) {
	rows := make([]Int, numRows)
	for i := range rows {
		rows[i].int64 = int64(i)
	}
	arg := Int{int64: 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range rows {
			_ = mulInt(rows[j], arg)
		}
	}
}
