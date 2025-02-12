package pro

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
)

var conn *pgxpool.Pool
var iro = New(
	cmp.Compare[int64],
	func(a, b int64) int64 { return a - b },
	func(a int64) int64 { return a + 1 },
	true,
)
var tro = NewTime()

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	// uses pool to try to connect to Docker
	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("postgres", "latest", []string{"POSTGRES_PASSWORD=secret"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		conn, err = pgxpool.New(
			context.Background(),
			fmt.Sprintf("postgres://postgres:secret@localhost:%s/postgres?sslmode=disable", resource.GetPort("5432/tcp")),
		)
		if err != nil {
			return err
		}
		return conn.Ping(context.Background())
	}); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	// as of go1.15 testing.M returns the exit code of m.Run(), so it is safe to use defer here
	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}

	}()

	m.Run()
}

func FuzzLessThan_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			firstIntRange := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			firstIntRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondIntRange := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			secondIntRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "<", "int8range", firstIntRange, secondIntRange, iro.LessThan)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "<", "tstzrange", firstTimeRange, secondTimeRange, tro.LessThan)
		},
	)
}

func FuzzLessThanOrEqualTo_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "<=", "int8range", first, second, iro.LessThanOrEqualTo)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "<=", "tstzrange", firstTimeRange, secondTimeRange, tro.LessThanOrEqualTo)
		},
	)
}

func FuzzGreaterThan_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, ">", "int8range", first, second, iro.GreaterThan)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, ">", "tstzrange", firstTimeRange, secondTimeRange, tro.GreaterThan)
		},
	)
}

func FuzzGreaterThanOrEqualTo_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, ">=", "int8range", first, second, iro.GreaterThanOrEqualTo)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, ">=", "tstzrange", firstTimeRange, secondTimeRange, tro.GreaterThanOrEqualTo)
		},
	)
}

func FuzzEqual_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "=", "int8range", first, second, iro.Equal)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "=", "tstzrange", firstTimeRange, secondTimeRange, tro.Equal)
		},
	)
}

func FuzzContain_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "@>", "int8range", first, second, iro.Contain)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "@>", "tstzrange", firstTimeRange, secondTimeRange, tro.Contain)
		},
	)
}

func FuzzContainElement_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, second int64) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))

			binaryOperatorTest2(t, "@>", "int8range", "bigint", first, second, iro.ContainElement)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTime := time.Unix(second, 0)

			binaryOperatorTest2(t, "@>", "tstzrange", "timestamp with time zone", firstTimeRange, secondTime, tro.ContainElement)
		},
	)
}

func FuzzOverlap_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "&&", "int8range", first, second, iro.Overlap)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "&&", "tstzrange", firstTimeRange, secondTimeRange, tro.Overlap)
		},
	)
}

func FuzzLeftOf_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "<<", "int8range", first, second, iro.LeftOf)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "<<", "tstzrange", firstTimeRange, secondTimeRange, tro.LeftOf)
		},
	)
}

func FuzzRightOf_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, ">>", "int8range", first, second, iro.RightOf)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, ">>", "tstzrange", firstTimeRange, secondTimeRange, tro.RightOf)
		},
	)
}

func FuzzAdjacent_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "-|-", "int8range", first, second, iro.Adjacent)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "-|-", "tstzrange", firstTimeRange, secondTimeRange, tro.Adjacent)
		},
	)
}

func FuzzIntersect_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest3(t, "*", "int8range", first, second, iro.Intersect)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest3(t, "*", "tstzrange", firstTimeRange, secondTimeRange, tro.Intersect)
		},
	)
}

func FuzzNotExtendRight_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "&<", "int8range", first, second, iro.NotExtendRight)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "&<", "tstzrange", firstTimeRange, secondTimeRange, tro.NotExtendRight)
		},
	)
}

func FuzzNotExtendLeft_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "&>", "int8range", first, second, iro.NotExtendLeft)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "&>", "tstzrange", firstTimeRange, secondTimeRange, tro.NotExtendLeft)
		},
	)
}

func FuzzUnion_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest3(t, "+", "int8range", first, second, iro.Union)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest3(t, "+", "tstzrange", firstTimeRange, secondTimeRange, tro.Union)
		},
	)
}

func FuzzMerge_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryFunctionTest(t, "range_merge", "int8range", first, second, iro.Merge)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryFunctionTest(t, "range_merge", "tstzrange", firstTimeRange, secondTimeRange, tro.Merge)
		},
	)
}

func FuzzDifference_(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int64, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int64, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int64]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int64]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest3(t, "-", "int8range", first, second, iro.Difference)

			firstTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerFirst, 0), Upper: time.Unix(upperFirst, 0), Valid: validFirst}
			firstTimeRange.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			secondTimeRange := pgtype.Range[time.Time]{Lower: time.Unix(lowerSecond, 0), Upper: time.Unix(upperSecond, 0), Valid: validSecond}
			secondTimeRange.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest3(t, "-", "tstzrange", firstTimeRange, secondTimeRange, tro.Difference)
		},
	)
}

func TestSize(t *testing.T) {
	tests := []struct {
		r           pgtype.Range[int64]
		expected    int64
		expectedErr bool
	}{
		{
			r:           pgtype.Range[int64]{Lower: 3, LowerType: pgtype.Inclusive, Upper: 6, UpperType: pgtype.Inclusive, Valid: false},
			expected:    0,
			expectedErr: true,
		},
		{
			r:           pgtype.Range[int64]{Lower: 0, LowerType: pgtype.Unbounded, Upper: 6, UpperType: pgtype.Inclusive, Valid: true},
			expected:    0,
			expectedErr: true,
		},
		{
			r:           pgtype.Range[int64]{Lower: 0, LowerType: pgtype.Unbounded, Upper: 0, UpperType: pgtype.Unbounded, Valid: true},
			expected:    0,
			expectedErr: true,
		},
		{
			r:           pgtype.Range[int64]{Lower: 100, LowerType: pgtype.Exclusive, Upper: 0, UpperType: pgtype.Unbounded, Valid: true},
			expected:    0,
			expectedErr: true,
		},
		{
			r:           pgtype.Range[int64]{Lower: 3, LowerType: pgtype.Inclusive, Upper: 6, UpperType: pgtype.Inclusive, Valid: true},
			expected:    4,
			expectedErr: false,
		},
		{
			r:           pgtype.Range[int64]{Lower: 3, LowerType: pgtype.Exclusive, Upper: 6, UpperType: pgtype.Inclusive, Valid: true},
			expected:    3,
			expectedErr: false,
		},
		{
			r:           pgtype.Range[int64]{Lower: 3, LowerType: pgtype.Inclusive, Upper: 6, UpperType: pgtype.Exclusive, Valid: true},
			expected:    3,
			expectedErr: false,
		},
		{
			r:           pgtype.Range[int64]{Lower: 3, LowerType: pgtype.Exclusive, Upper: 6, UpperType: pgtype.Exclusive, Valid: true},
			expected:    2,
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		result, err := iro.Size(tt.r)
		if err == nil && tt.expectedErr {
			t.Errorf("size `%v`: expected error, got none", tt.r)
		}
		if err != nil && !tt.expectedErr {
			t.Errorf("size `%v`: expected no error, got `%v`", tt.r, err)
		}
		if err != nil && tt.expectedErr {
			return
		}
		if tt.expected != result {
			t.Errorf("size `%v`: expected result `%v`, got `%v`", tt.r, tt.expected, result)
		}
	}
}

func binaryOperatorTest1[T any](t *testing.T, sqlOperator, sqlRangeType string, first, second pgtype.Range[T], fn func(pgtype.Range[T], pgtype.Range[T]) (bool, error)) {
	expected, expectedErr := retrieveExpected[bool](
		fmt.Sprintf(`SELECT @first::%s %s @second::%s`, sqlRangeType, sqlOperator, sqlRangeType),
		pgx.NamedArgs{"first": first, "second": second},
	)
	result, err := fn(first, second)
	if err == nil && expectedErr != nil {
		t.Errorf("`%v` %s `%v`: expected error `%v`, got none", first, sqlOperator, second, expectedErr)
	}
	if err != nil && expectedErr == nil {
		t.Errorf("`%v` %s `%v`: expected no error, got `%v`", first, sqlOperator, second, err)
	}
	if err != nil && expectedErr != nil {
		return
	}
	if expected != result {
		t.Errorf("`%v` %s `%v`: expected result `%v`, got `%v`", first, sqlOperator, second, expected, result)
	}
}

func binaryOperatorTest2[T any](t *testing.T, sqlOperator, sqlRangeType, sqlElementType string, first pgtype.Range[T], second T, fn func(pgtype.Range[T], T) (bool, error)) {
	expected, expectedErr := retrieveExpected[bool](
		fmt.Sprintf(`SELECT @first::%s %s @second::%s`, sqlRangeType, sqlOperator, sqlElementType),
		pgx.NamedArgs{"first": first, "second": second},
	)
	result, err := fn(first, second)
	if err == nil && expectedErr != nil {
		t.Errorf("`%v` %s `%v`: expected error `%v`, got none", first, sqlOperator, second, expectedErr)
	}
	if err != nil && expectedErr == nil {
		t.Errorf("`%v` %s `%v`: expected no error, got `%v`", first, sqlOperator, second, err)
	}
	if err != nil && expectedErr != nil {
		return
	}
	if expected != result {
		t.Errorf("`%v` %s `%v`: expected result `%v`, got `%v`", first, sqlOperator, second, expected, result)
	}
}

func binaryOperatorTest3[T any](t *testing.T, sqlOperator, sqlRangeType string, first, second pgtype.Range[T], fn func(pgtype.Range[T], pgtype.Range[T]) (pgtype.Range[T], error)) {
	expected, expectedErr := retrieveExpected[pgtype.Range[T]](
		fmt.Sprintf(`SELECT @first::%s %s @second::%s`, sqlRangeType, sqlOperator, sqlRangeType),
		pgx.NamedArgs{"first": first, "second": second},
	)
	result, err := fn(first, second)
	if err == nil && expectedErr != nil {
		t.Errorf("`%v` %s `%v`: expected error `%v`, got none", first, sqlOperator, second, expectedErr)
	}
	if err != nil && expectedErr == nil {
		t.Errorf("`%v` %s `%v`: expected no error, got `%v`", first, sqlOperator, second, err)
	}
	if err != nil && expectedErr != nil {
		return
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("`%v` %s `%v`: expected result `%v`, got `%v`", first, sqlOperator, second, expected, result)
	}
}

func binaryFunctionTest[T any](t *testing.T, sqlFunction, sqlRangeType string, first, second pgtype.Range[T], fn func(pgtype.Range[T], pgtype.Range[T]) (pgtype.Range[T], error)) {
	expected, expectedErr := retrieveExpected[pgtype.Range[T]](
		fmt.Sprintf(`SELECT %s(@first::%s, @second::%s)`, sqlFunction, sqlRangeType, sqlRangeType),
		pgx.NamedArgs{"first": first, "second": second},
	)
	result, err := fn(first, second)
	if err == nil && expectedErr != nil {
		t.Errorf("%s(`%v`, `%v`): expected error `%v`, got none", sqlFunction, first, second, expectedErr)
	}
	if err != nil && expectedErr == nil {
		t.Errorf("%s(`%v`, `%v`): expected no error, got `%v`", sqlFunction, first, second, err)
	}
	if err != nil && expectedErr != nil {
		return
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("%s(`%v`, `%v`): expected result `%v`, got `%v`", sqlFunction, first, second, expected, result)
	}
}

func retrieveExpected[T any](query string, args pgx.NamedArgs) (T, error) {
	rows, err := conn.Query(
		context.Background(),
		query,
		args,
	)
	if err != nil {
		return *new(T), fmt.Errorf("excuting query failed: %v", err)
	}
	defer rows.Close()
	expected, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[T])
	if err != nil {
		return *new(T), fmt.Errorf("collecting the row failed: %v", err)
	}
	return expected, nil
}

func sort(lower, upper int64) (int64, int64) {
	if lower > upper {
		return upper, lower
	}
	return lower, upper
}

func createBoundType(i int64) pgtype.BoundType {
	types := []pgtype.BoundType{
		pgtype.Inclusive,
		pgtype.Exclusive,
		pgtype.Unbounded,
		// todo: what to do with pgtype.Empty
		// pgtype.Empty,
	}
	i %= int64(len(types))
	if i < 0 {
		i = -i
	}
	return types[i]
}
