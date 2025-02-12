# pgx Range operators
This package adds the Postgresql range operators and functions to Go. It uses the [pgx Range type](https://pkg.go.dev/github.com/jackc/pgx/v5/pgtype#Range).
## Installation
```sh
go get github.com/munnik/pgx_range_operator
```
## Usage
```go
package main

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	pro "github.com/munnik/pgx_range_operator"
)

func main() {
	first := pgtype.Range[int]{
		Lower:     -3,
		LowerType: pgtype.Exclusive,
		Upper:     5,
		UpperType: pgtype.Inclusive,
		Valid:     true,
	}
	second := pgtype.Range[int]{
		Lower:     2,
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Unbounded,
		Valid:     true,
	}

	ro := pro.NewInteger()
	overlap, err := ro.Overlap(first, second)
	if err != nil {
		panic(err)
	}
	if overlap {
		fmt.Printf("The ranges %v and %v overlap\n", first, second)
	} else {
		fmt.Printf("The ranges %v and %v do not overlap\n", first, second)
	}
}
```
