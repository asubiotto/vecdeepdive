# vecdeepdive
This is the home for the companion code to CockroachDB's vectorized deep dive blog post. There are a couple of versions of a operators that perform multiplication and benchmarks for each version.
## Download
`go get github.com/asubiotto/vecdeepdive`
## Run benchmarks
`go test -bench .`
## Generate code
NOTE: Only for the tuple-at-a-time concrete type operator.

From the project root: `rm row_based_typed.gen.go && go run .`
