package pro

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
)

var conn *pgxpool.Pool
var integerRangeOperator = NewInteger()
var timeRangeOperator = NewTime()

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

func FuzzLessThan(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "<", first, second, integerRangeOperator.LessThan)
		},
	)
}

func FuzzLessThanOrEqualTo(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "<=", first, second, integerRangeOperator.LessThanOrEqualTo)
		},
	)
}

func FuzzGreaterThan(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, ">", first, second, integerRangeOperator.GreaterThan)
		},
	)
}

func FuzzGreaterThanOrEqualTo(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, ">=", first, second, integerRangeOperator.GreaterThanOrEqualTo)
		},
	)
}

func FuzzEqual(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "=", first, second, integerRangeOperator.Equal)
		},
	)
}

func FuzzContain(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "@>", first, second, integerRangeOperator.Contain)
		},
	)
}

func FuzzContainElement(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, second int) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))

			binaryOperatorTest2(t, "@>", first, second, integerRangeOperator.ContainElement)
		},
	)
}

func FuzzOverlap(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "&&", first, second, integerRangeOperator.Overlap)
		},
	)
}

func FuzzLeftOf(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "<<", first, second, integerRangeOperator.LeftOf)
		},
	)
}

func FuzzRightOf(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, ">>", first, second, integerRangeOperator.RightOf)
		},
	)
}

func FuzzAdjacent(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "-|-", first, second, integerRangeOperator.Adjacent)
		},
	)
}

func FuzzIntersect(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest3(t, "*", first, second, integerRangeOperator.Intersect)
		},
	)
}

func FuzzNotExtendRight(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "&<", first, second, integerRangeOperator.NotExtendRight)
		},
	)
}

func FuzzNotExtendLeft(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest1(t, "&>", first, second, integerRangeOperator.NotExtendLeft)
		},
	)
}

func FuzzUnion(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest3(t, "+", first, second, integerRangeOperator.Union)
		},
	)
}

func FuzzMerge(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryFunctionTest(t, "range_merge", first, second, integerRangeOperator.Merge)
		},
	)
}

func FuzzDifference(f *testing.F) {
	f.Fuzz(
		func(t *testing.T, lowerFirst, lowerTypeFirst, upperFirst, upperTypeFirst int, validFirst bool, lowerSecond, lowerTypeSecond, upperSecond, upperTypeSecond int, validSecond bool) {
			t.Parallel()

			lowerFirst, upperFirst = sort(lowerFirst, upperFirst)
			lowerSecond, upperSecond = sort(lowerSecond, upperSecond)

			first := pgtype.Range[int]{Lower: lowerFirst, Upper: upperFirst, Valid: validFirst}
			first.SetBoundTypes(createBoundType(lowerTypeFirst), createBoundType(upperTypeFirst))
			second := pgtype.Range[int]{Lower: lowerSecond, Upper: upperSecond, Valid: validSecond}
			second.SetBoundTypes(createBoundType(lowerTypeSecond), createBoundType(upperTypeSecond))

			binaryOperatorTest3(t, "-", first, second, integerRangeOperator.Difference)
		},
	)
}

func TestSize(t *testing.T) {
	tests := []struct {
		r           pgtype.Range[int]
		expected    int
		expectedErr bool
	}{
		{
			r:           pgtype.Range[int]{Lower: 3, LowerType: pgtype.Inclusive, Upper: 6, UpperType: pgtype.Inclusive, Valid: false},
			expected:    0,
			expectedErr: true,
		},
		{
			r:           pgtype.Range[int]{Lower: 0, LowerType: pgtype.Unbounded, Upper: 6, UpperType: pgtype.Inclusive, Valid: true},
			expected:    0,
			expectedErr: true,
		},
		{
			r:           pgtype.Range[int]{Lower: 0, LowerType: pgtype.Unbounded, Upper: 0, UpperType: pgtype.Unbounded, Valid: true},
			expected:    0,
			expectedErr: true,
		},
		{
			r:           pgtype.Range[int]{Lower: 100, LowerType: pgtype.Exclusive, Upper: 0, UpperType: pgtype.Unbounded, Valid: true},
			expected:    0,
			expectedErr: true,
		},
		{
			r:           pgtype.Range[int]{Lower: 3, LowerType: pgtype.Inclusive, Upper: 6, UpperType: pgtype.Inclusive, Valid: true},
			expected:    4,
			expectedErr: false,
		},
		{
			r:           pgtype.Range[int]{Lower: 3, LowerType: pgtype.Exclusive, Upper: 6, UpperType: pgtype.Inclusive, Valid: true},
			expected:    3,
			expectedErr: false,
		},
		{
			r:           pgtype.Range[int]{Lower: 3, LowerType: pgtype.Inclusive, Upper: 6, UpperType: pgtype.Exclusive, Valid: true},
			expected:    3,
			expectedErr: false,
		},
		{
			r:           pgtype.Range[int]{Lower: 3, LowerType: pgtype.Exclusive, Upper: 6, UpperType: pgtype.Exclusive, Valid: true},
			expected:    2,
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		result, err := integerRangeOperator.Size(tt.r)
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

func binaryOperatorTest1(t *testing.T, sqlOperator string, first, second pgtype.Range[int], fn func(pgtype.Range[int], pgtype.Range[int]) (bool, error)) {
	expected, expectedErr := retrieveExpected[bool](
		fmt.Sprintf(`SELECT @first::int4range %s @second::int4range`, sqlOperator),
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

func binaryOperatorTest2(t *testing.T, sqlOperator string, first pgtype.Range[int], second int, fn func(pgtype.Range[int], int) (bool, error)) {
	expected, expectedErr := retrieveExpected[bool](
		fmt.Sprintf(`SELECT @first::int4range %s @second::integer`, sqlOperator),
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

func binaryOperatorTest3(t *testing.T, sqlOperator string, first, second pgtype.Range[int], fn func(pgtype.Range[int], pgtype.Range[int]) (pgtype.Range[int], error)) {
	expected, expectedErr := retrieveExpected[pgtype.Range[int]](
		fmt.Sprintf(`SELECT @first::int4range %s @second::int4range`, sqlOperator),
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

func binaryFunctionTest(t *testing.T, sqlFunction string, first, second pgtype.Range[int], fn func(pgtype.Range[int], pgtype.Range[int]) (pgtype.Range[int], error)) {
	expected, expectedErr := retrieveExpected[pgtype.Range[int]](
		fmt.Sprintf(`SELECT %s(@first::int4range, @second::int4range)`, sqlFunction),
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
	if expected != result {
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

func sort(lower, upper int) (int, int) {
	if lower > upper {
		return upper, lower
	}
	return lower, upper
}

func createBoundType(i int) pgtype.BoundType {
	types := []pgtype.BoundType{
		pgtype.Inclusive,
		pgtype.Exclusive,
		pgtype.Unbounded,
		// todo: what to do with pgtype.Empty
		// pgtype.Empty,
	}
	i %= len(types)
	if i < 0 {
		i = -i
	}
	return types[i]
}
