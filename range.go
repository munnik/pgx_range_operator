package pro

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/constraints"
)

type Range[T any, S constraints.Integer] struct {
	r  pgtype.Range[T]
	ro operator[T, S]
}

type RangeOption[T any, S constraints.Integer] func(*Range[T, S])

func WithLowerType[T any, S constraints.Integer](t pgtype.BoundType) RangeOption[T, S] {
	return func(r *Range[T, S]) {
		r.r.LowerType = t
	}
}

func WithLowerInf[T any, S constraints.Integer]() RangeOption[T, S] {
	return func(r *Range[T, S]) {
		r.r.Lower = r.ro.zero
		r.r.LowerType = pgtype.Unbounded
	}
}

func WithUpperType[T any, S constraints.Integer](t pgtype.BoundType) RangeOption[T, S] {
	return func(r *Range[T, S]) {
		r.r.UpperType = t
	}
}

func WithUpperInf[T any, S constraints.Integer]() RangeOption[T, S] {
	return func(r *Range[T, S]) {
		r.r.Lower = r.ro.zero
		r.r.LowerType = pgtype.Unbounded
	}
}

func WithInvalid[T any, S constraints.Integer]() RangeOption[T, S] {
	return func(r *Range[T, S]) {
		r.r.Valid = false
	}
}

type TimeRange = Range[time.Time, time.Duration]
type IntegerRange = Range[int, int]

func NewIntegerRange(lower, upper int, opts ...RangeOption[int, int]) IntegerRange {
	result := &IntegerRange{
		r: pgtype.Range[int]{
			Lower:     lower,
			LowerType: pgtype.Inclusive,
			Upper:     upper,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		ro: NewInteger(),
	}
	for _, opt := range opts {
		opt(result)
	}
	return *result
}

func NewTimeRange(lower, upper time.Time, opts ...RangeOption[time.Time, time.Duration]) TimeRange {
	result := &TimeRange{
		r: pgtype.Range[time.Time]{
			Lower:     lower,
			LowerType: pgtype.Inclusive,
			Upper:     upper,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		ro: NewTime(),
	}
	for _, opt := range opts {
		opt(result)
	}
	return *result
}

// Implement RangeValuer interface
func (r Range[T, S]) IsNull() bool {
	return r.r.IsNull()
}

func (r Range[T, S]) BoundTypes() (lower, upper pgtype.BoundType) {
	return r.r.BoundTypes()
}

func (r Range[T, S]) Bounds() (lower, upper any) {
	return r.r.Bounds()
}

// Implement RangeScanner interface
func (r *Range[T, S]) ScanNull() error {
	*r = Range[T, S]{}
	return nil
}

func (r *Range[T, S]) ScanBounds() (lowerTarget, upperTarget any) {
	return r.r.ScanBounds()
}

func (r *Range[T, S]) SetBoundTypes(lower, upper pgtype.BoundType) error {
	return r.r.SetBoundTypes(lower, upper)
}

// Implement operators and functions
func (r Range[T, S]) Empty() (bool, error) {
	return r.ro.Empty(r.r)
}

func (r Range[T, S]) Lower() (T, error) {
	if r.LowerInf() {
		return r.ro.zero, fmt.Errorf("lower bound is infinite")
	}
	if r.r.LowerType == pgtype.Empty {
		return r.ro.zero, fmt.Errorf("lower bound is empty")
	}
	return r.r.Lower, nil
}

func (r Range[T, S]) LowerInf() bool {
	return r.ro.LowerInf(r.r)
}

func (r *Range[T, S]) SetLower(v T) *Range[T, S] {
	r.r.Lower = v
	return r
}

func (r *Range[T, S]) SetLowerBoundType(v pgtype.BoundType) *Range[T, S] {
	r.r.LowerType = v
	if r.r.LowerType == pgtype.Empty || r.r.UpperType == pgtype.Empty {
		r.r.Valid = false
	} else {
		r.r.Valid = true
	}
	return r
}

func (r *Range[T, S]) SetLowerInf() *Range[T, S] {
	r.r.Lower = r.ro.zero
	r.r.LowerType = pgtype.Unbounded
	return r
}

func (r Range[T, S]) Upper() (T, error) {
	if r.UpperInf() {
		return r.ro.zero, fmt.Errorf("upper bound is infinite")
	}
	if r.r.UpperType == pgtype.Empty {
		return r.ro.zero, fmt.Errorf("upper bound is empty")
	}
	return r.r.Upper, nil
}

func (r Range[T, S]) UpperInf() bool {
	return r.ro.UpperInf(r.r)
}

func (r *Range[T, S]) SetUpper(v T) *Range[T, S] {
	r.r.Upper = v
	return r
}

func (r *Range[T, S]) SetUpperBoundType(v pgtype.BoundType) *Range[T, S] {
	r.r.UpperType = v
	if r.r.LowerType == pgtype.Empty || r.r.UpperType == pgtype.Empty {
		r.r.Valid = false
	} else {
		r.r.Valid = true
	}
	return r
}

func (r *Range[T, S]) SetUpperInf() *Range[T, S] {
	r.r.Upper = r.ro.zero
	r.r.UpperType = pgtype.Unbounded
	return r
}

// Is the first range equal to the second?
// PostgreSQL equivalent: anyrange = anyrange → boolean
func (r Range[T, S]) Equal(other Range[T, S]) (bool, error) {
	return r.ro.Equal(r.r, other.r)
}

// Is the first range less than the second?
// PostgreSQL equivalent: anyrange < anyrange → boolean
func (r Range[T, S]) LessThan(other Range[T, S]) (bool, error) {
	return r.ro.LessThan(r.r, other.r)
}

// Is the first range ess than or equal to the second?
// PostgreSQL equivalent: anyrange <= anyrange → boolean
func (r Range[T, S]) LessThanOrEqualTo(other Range[T, S]) (bool, error) {
	return r.ro.LessThanOrEqualTo(r.r, other.r)
}

// Is the first range less than the second?
// PostgreSQL equivalent: anyrange > anyrange → boolean
func (r Range[T, S]) GreaterThan(other Range[T, S]) (bool, error) {
	return r.ro.GreaterThan(r.r, other.r)
}

// Is the first range ess than or equal to the second?
// PostgreSQL equivalent: anyrange >= anyrange → boolean
func (r Range[T, S]) GreaterThanOrEqualTo(other Range[T, S]) (bool, error) {
	return r.ro.GreaterThanOrEqualTo(r.r, other.r)
}

// Does the first range contain the second?
// PostgreSQL equivalent: anyrange @> anyrange → boolean
func (r Range[T, S]) Contain(other Range[T, S]) (bool, error) {
	return r.ro.Contain(r.r, other.r)
}

// Does the range contain the element?
// PostgreSQL equivalent: anyrange @> anyelement → boolean
func (r Range[T, S]) ContainElement(elem T) (bool, error) {
	return r.ro.ContainElement(r.r, elem)
}

// Do the ranges overlap, that is, have any elements in common?
// PostgreSQL equivalent: anyrange && anyrange → boolean
func (r Range[T, S]) Overlap(other Range[T, S]) (bool, error) {
	return r.ro.Overlap(r.r, other.r)
}

// Is the first range strictly left of the second?
// PostgreSQL equivalent: anyrange << anyrange → boolean
func (r Range[T, S]) LeftOf(other Range[T, S]) (bool, error) {
	return r.ro.LeftOf(r.r, other.r)
}

// Is the first range strictly right of the second?
// PostgreSQL equivalent: anyrange >> anyrange → boolean
func (r Range[T, S]) RightOf(other Range[T, S]) (bool, error) {
	return r.ro.RightOf(r.r, other.r)
}

// Does the first range not extend to the right of the second?
// PostgreSQL equivalent: anyrange &< anyrange → boolean
func (r Range[T, S]) NotExtendRight(other Range[T, S]) (bool, error) {
	return r.ro.NotExtendRight(r.r, other.r)
}

// Does the first range not extend to the left of the second?
// PostgreSQL equivalent: anyrange &> anyrange → boolean
func (r Range[T, S]) NotExtendLeft(other Range[T, S]) (bool, error) {
	return r.ro.NotExtendLeft(r.r, other.r)
}

// Are the ranges adjacent?
// PostgreSQL equivalent: anyrange -|- anyrange → boolean
func (r Range[T, S]) Adjacent(other Range[T, S]) (bool, error) {
	return r.ro.Adjacent(r.r, other.r)
}

func (r Range[T, S]) Union(other Range[T, S]) (Range[T, S], error) {
	result, err := r.ro.Union(r.r, other.r)
	r.r = result
	return r, err
}

func (r Range[T, S]) Merge(other Range[T, S]) (Range[T, S], error) {
	result, err := r.ro.Merge(r.r, other.r)
	r.r = result
	return r, err
}

// Computes the intersection of the ranges.
// PostgreSQL equivalent: anyrange * anyrange → anyrange
func (r Range[T, S]) Intersect(other Range[T, S]) (Range[T, S], error) {
	result, err := r.ro.Intersect(r.r, other.r)
	r.r = result
	return r, err
}

func (r Range[T, S]) Difference(other Range[T, S]) (Range[T, S], error) {
	result, err := r.ro.Difference(r.r, other.r)
	r.r = result
	return r, err
}

func (r Range[T, S]) Size() (S, error) {
	return r.ro.Size(r.r)
}

func (r Range[T, S]) Rewrite() Range[T, S] {
	result := r.ro.Rewrite(r.r)
	r.r = result
	return r
}
