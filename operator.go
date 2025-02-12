package pro

import (
	"cmp"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/exp/constraints"
)

type operator[T any, S constraints.Integer] struct {
	cmp    func(a, b T) int
	diff   func(a, b T) S
	addOne func(a T) T
	zero   T
}

// Create a new operator for the Range[T] type
//
// The cmp function is used to compare two values of type T, the function should return
// -1 if a < b, 0 if a == b and 1 if a > b.
//
// The diff function is used to calculate the difference between to values of type T, the
// function should return a -b. The return type of this function is S.
//
// Also see the functions [pgxrangeoperator.NewInteger] and [pgxrangeoperator.NewTime]
func New[T any, S constraints.Integer](cmp func(a, b T) int, diff func(a, b T) S, addOne func(a T) T) operator[T, S] {
	return operator[T, S]{
		cmp:    cmp,
		diff:   diff,
		addOne: addOne,
		zero:   *new(T),
	}
}

func NewInteger() operator[int, int] {
	return operator[int, int]{
		cmp:    cmp.Compare[int],
		diff:   func(a, b int) int { return a - b },
		addOne: func(a int) int { return a + 1 },
		zero:   0,
	}
}

func NewTime() operator[time.Time, time.Duration] {
	return operator[time.Time, time.Duration]{
		cmp: func(a, b time.Time) int {
			if a.Before(b) {
				return -1
			} else if a.Equal(b) {
				return 0
			}
			return 1
		},
		diff: func(a, b time.Time) time.Duration {
			return a.Sub(b)
		},
		addOne: func(a time.Time) time.Time {
			return a.Add(time.Duration(1))
		},
		zero: *new(time.Time),
	}
}

func (ro operator[T, S]) Empty(r pgtype.Range[T]) (bool, error) {
	if !r.Valid {
		return false, fmt.Errorf("range is not valid")
	}
	if r.LowerType == pgtype.Unbounded || r.UpperType == pgtype.Unbounded {
		return false, nil
	}
	s, _ := ro.Size(r)
	return r.LowerType == pgtype.Empty || r.UpperType == pgtype.Empty || s <= 0, nil
}

func (ro operator[T, S]) LowerInf(r pgtype.Range[T]) bool {
	return r.LowerType == pgtype.Unbounded
}

func (ro operator[T, S]) UpperInf(r pgtype.Range[T]) bool {
	return r.LowerType == pgtype.Unbounded
}

// Is the first range equal to the second?
// PostgreSQL equivalent: anyrange = anyrange → boolean
func (ro operator[T, S]) Equal(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)
	if firstEmpty && secondEmpty {
		return true, nil
	}
	if firstEmpty != secondEmpty {
		return false, nil
	}

	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	if ro.compareBounds(first, second, true, true) != 0 {
		return false, nil
	}

	if ro.compareBounds(first, second, false, false) != 0 {
		return false, nil
	}

	return true, nil
}

// Is the first range less than the second?
// PostgreSQL equivalent: anyrange < anyrange → boolean
func (ro operator[T, S]) LessThan(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	return ro.compareRanges(first, second) < 0, nil
}

// Is the first range ess than or equal to the second?
// PostgreSQL equivalent: anyrange <= anyrange → boolean
func (ro operator[T, S]) LessThanOrEqualTo(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	return ro.compareRanges(first, second) <= 0, nil
}

// Is the first range less than the second?
// PostgreSQL equivalent: anyrange > anyrange → boolean
func (ro operator[T, S]) GreaterThan(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	return ro.compareRanges(first, second) > 0, nil
}

// Is the first range ess than or equal to the second?
// PostgreSQL equivalent: anyrange >= anyrange → boolean
func (ro operator[T, S]) GreaterThanOrEqualTo(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	return ro.compareRanges(first, second) >= 0, nil
}

// Does the first range contain the second?
// PostgreSQL equivalent: anyrange @> anyrange → boolean
func (ro operator[T, S]) Contain(first, second pgtype.Range[T]) (bool, error) {
	intersect, err := ro.Intersect(first, second)
	if err != nil {
		return false, err
	}
	return ro.Equal(intersect, second)
}

// Does the range contain the element?
// PostgreSQL equivalent: anyrange @> anyelement → boolean
func (ro operator[T, S]) ContainElement(first pgtype.Range[T], elem T) (bool, error) {
	second := pgtype.Range[T]{Lower: elem, Upper: elem, Valid: true}
	second.SetBoundTypes(pgtype.Inclusive, pgtype.Inclusive)
	return ro.Contain(first, second)
}

// Do the ranges overlap, that is, have any elements in common?
// PostgreSQL equivalent: anyrange && anyrange → boolean
func (ro operator[T, S]) Overlap(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)
	if firstEmpty || secondEmpty {
		return false, nil
	}

	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	if ro.compareBounds(first, second, true, true) >= 0 && ro.compareBounds(first, second, true, false) <= 0 {
		return true, nil
	}
	if ro.compareBounds(second, first, true, true) >= 0 && ro.compareBounds(second, first, true, false) <= 0 {
		return true, nil
	}

	return false, nil
}

// Is the first range strictly left of the second?
// PostgreSQL equivalent: anyrange << anyrange → boolean
func (ro operator[T, S]) LeftOf(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)
	if firstEmpty || secondEmpty {
		return false, nil
	}

	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	return ro.compareBounds(first, second, false, true) < 0, nil
}

// Is the first range strictly right of the second?
// PostgreSQL equivalent: anyrange >> anyrange → boolean
func (ro operator[T, S]) RightOf(first, second pgtype.Range[T]) (bool, error) {
	return ro.LeftOf(second, first)
}

// Does the first range not extend to the right of the second?
// PostgreSQL equivalent: anyrange &< anyrange → boolean
func (ro operator[T, S]) NotExtendRight(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)
	if firstEmpty || secondEmpty {
		return false, nil
	}

	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	return ro.compareBounds(first, second, false, false) <= 0, nil
}

// Does the first range not extend to the left of the second?
// PostgreSQL equivalent: anyrange &> anyrange → boolean
func (ro operator[T, S]) NotExtendLeft(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)
	if firstEmpty || secondEmpty {
		return false, nil
	}

	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	return ro.compareBounds(first, second, true, true) >= 0, nil
}

// Are the ranges adjacent?
// PostgreSQL equivalent: anyrange -|- anyrange → boolean
func (ro operator[T, S]) Adjacent(first, second pgtype.Range[T]) (bool, error) {
	if !first.Valid {
		return false, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return false, fmt.Errorf("second range is not valid")
	}

	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)
	if firstEmpty || secondEmpty {
		return false, nil
	}

	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	if ((first.UpperType == pgtype.Inclusive && second.LowerType == pgtype.Exclusive) ||
		(first.UpperType == pgtype.Exclusive && second.LowerType == pgtype.Inclusive)) &&
		ro.cmp(first.Upper, second.Lower) == 0 {
		return true, nil
	}
	if ((first.LowerType == pgtype.Inclusive && second.UpperType == pgtype.Exclusive) ||
		(first.LowerType == pgtype.Exclusive && second.UpperType == pgtype.Inclusive)) &&
		ro.cmp(first.Lower, second.Upper) == 0 {
		return true, nil
	}
	return false, nil
}

func (ro operator[T, S]) Union(first, second pgtype.Range[T]) (pgtype.Range[T], error) {
	return ro.union(first, second, true)
}

func (ro operator[T, S]) Merge(first, second pgtype.Range[T]) (pgtype.Range[T], error) {
	return ro.union(first, second, false)
}

func (ro operator[T, S]) union(first, second pgtype.Range[T], strict bool) (pgtype.Range[T], error) {
	if !first.Valid {
		return pgtype.Range[T]{}, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return pgtype.Range[T]{}, fmt.Errorf("second range is not valid")
	}

	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)
	if firstEmpty && secondEmpty {
		return makeEmptyRange[T](), nil
	}
	if firstEmpty {
		return second, nil
	}
	if secondEmpty {
		return first, nil
	}

	overlap, _ := ro.Overlap(first, second)
	adjacent, _ := ro.Adjacent(first, second)
	if !overlap && !adjacent && strict {
		return pgtype.Range[T]{}, fmt.Errorf("result of range union would not be contiguous")
	}

	result := pgtype.Range[T]{
		Valid: true,
	}
	if ro.compareBounds(first, second, true, true) < 0 {
		result.Lower = first.Lower
		result.LowerType = first.LowerType
	} else {
		result.Lower = second.Lower
		result.LowerType = second.LowerType
	}
	if ro.compareBounds(first, second, false, false) > 0 {
		result.Upper = first.Upper
		result.UpperType = first.UpperType
	} else {
		result.Upper = second.Upper
		result.UpperType = second.UpperType
	}

	return ro.Rewrite(result), nil
}

// Computes the intersection of the ranges.
// PostgreSQL equivalent: anyrange * anyrange → anyrange
func (ro operator[T, S]) Intersect(first, second pgtype.Range[T]) (pgtype.Range[T], error) {
	if !first.Valid {
		return pgtype.Range[T]{}, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return pgtype.Range[T]{}, fmt.Errorf("second range is not valid")
	}

	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)
	overlap, _ := ro.Overlap(first, second)
	if firstEmpty || secondEmpty || !overlap {
		return makeEmptyRange[T](), nil
	}

	result := pgtype.Range[T]{
		Valid: true,
	}
	if ro.compareBounds(first, second, true, true) >= 0 {
		result.Lower = first.Lower
		result.LowerType = first.LowerType
	} else {
		result.Lower = second.Lower
		result.LowerType = second.LowerType
	}
	if ro.compareBounds(first, second, false, false) <= 0 {
		result.Upper = first.Upper
		result.UpperType = first.UpperType
	} else {
		result.Upper = second.Upper
		result.UpperType = second.UpperType
	}

	return ro.Rewrite(result), nil
}

func (ro operator[T, S]) Difference(first, second pgtype.Range[T]) (pgtype.Range[T], error) {
	if !first.Valid {
		return pgtype.Range[T]{}, fmt.Errorf("first range is not valid")
	}
	if !second.Valid {
		return pgtype.Range[T]{}, fmt.Errorf("second range is not valid")
	}

	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)
	if firstEmpty {
		return makeEmptyRange[T](), nil
	}
	if secondEmpty {
		return ro.Rewrite(first), nil
	}

	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	l1l2 := ro.compareBounds(first, second, true, true)
	l1u2 := ro.compareBounds(first, second, true, false)
	u1l2 := ro.compareBounds(first, second, false, true)
	u1u2 := ro.compareBounds(first, second, false, false)

	if l1l2 < 0 && u1u2 > 0 {
		// cut in the middle
		return pgtype.Range[T]{}, fmt.Errorf("result of range difference would not be contiguous")
	}

	if l1u2 > 0 || u1l2 < 0 {
		// no overlap
		return ro.Rewrite(first), nil
	}

	if l1l2 >= 0 && u1u2 <= 0 {
		// at least full overlap
		return makeEmptyRange[T](), nil
	}

	if l1l2 <= 0 && u1l2 >= 0 && u1u2 <= 0 {
		inverseUpperType := pgtype.Exclusive
		if second.LowerType == pgtype.Exclusive {
			inverseUpperType = pgtype.Inclusive
		}
		return pgtype.Range[T]{
			Lower:     first.Lower,
			LowerType: first.LowerType,
			Upper:     second.Lower,
			UpperType: inverseUpperType,
			Valid:     true,
		}, nil
	}

	if l1l2 >= 0 && u1u2 >= 0 && l1u2 <= 0 {
		inverseUpperType := pgtype.Exclusive
		if second.UpperType == pgtype.Exclusive {
			inverseUpperType = pgtype.Inclusive
		}
		return pgtype.Range[T]{
			Lower:     second.Upper,
			LowerType: inverseUpperType,
			Upper:     first.Upper,
			UpperType: first.UpperType,
			Valid:     true,
		}, nil
	}

	return pgtype.Range[T]{}, fmt.Errorf("unexpected case in range difference")
}

func (ro operator[T, S]) Size(r pgtype.Range[T]) (S, error) {
	if !r.Valid {
		return ro.diff(ro.zero, ro.zero), fmt.Errorf("the range is not valid")
	}

	if r.LowerType == pgtype.Unbounded || r.UpperType == pgtype.Unbounded {
		return ro.diff(ro.zero, ro.zero), fmt.Errorf("the range is unbounded")
	}
	if r.LowerType == pgtype.Exclusive {
		r.Lower = ro.addOne(r.Lower)
		r.LowerType = pgtype.Inclusive
	}
	if r.UpperType == pgtype.Inclusive {
		r.Upper = ro.addOne(r.Upper)
		r.UpperType = pgtype.Exclusive
	}
	return ro.diff(r.Upper, r.Lower), nil
}

// Rewrite converts all bounded ranges to the form [ , )
func (ro operator[T, S]) Rewrite(r pgtype.Range[T]) pgtype.Range[T] {
	if r.LowerType == pgtype.Exclusive {
		r.Lower = ro.addOne(r.Lower)
		r.LowerType = pgtype.Inclusive
	}
	if r.UpperType == pgtype.Inclusive {
		r.Upper = ro.addOne(r.Upper)
		r.UpperType = pgtype.Exclusive
	}

	if e, _ := ro.Empty(r); e {
		return makeEmptyRange[T]()
	}

	return r
}

func (ro operator[T, S]) compareRanges(first, second pgtype.Range[T]) int {
	first = ro.Rewrite(first)
	second = ro.Rewrite(second)

	result := 0
	firstEmpty, _ := ro.Empty(first)
	secondEmpty, _ := ro.Empty(second)

	if firstEmpty && secondEmpty {
		result = 0
	} else if firstEmpty {
		result = -1
	} else if secondEmpty {
		result = 1
	} else {
		result = ro.compareBounds(first, second, true, true)
		if result == 0 {
			result = ro.compareBounds(first, second, false, false)
		}
	}
	return result
}

// the boolean parameters determine if the lower or upper bound is used to for comparison
func (ro operator[T, S]) compareBounds(first, second pgtype.Range[T], firstLower, secondLower bool) int {
	// make sure the boundaries that need to be compared are in the lower part of the ranges
	// this makes the rest of the code easier to understand
	if !firstLower {
		first.Lower = first.Upper
		first.LowerType = first.UpperType
	}
	if !secondLower {
		second.Lower = second.Upper
		second.LowerType = second.UpperType
	}

	if first.LowerType == pgtype.Unbounded && second.LowerType == pgtype.Unbounded {
		if firstLower == secondLower {
			return 0
		}
		if firstLower {
			return -1
		}
		return 1
	} else if first.LowerType == pgtype.Unbounded {
		if firstLower {
			return -1
		}
		return 1
	} else if second.LowerType == pgtype.Unbounded {
		if secondLower {
			return 1
		}
		return -1
	}

	result := ro.cmp(first.Lower, second.Lower)
	if result == 0 {
		if first.LowerType != pgtype.Inclusive && second.LowerType != pgtype.Inclusive {
			if firstLower == secondLower {
				return 0
			}
			if firstLower {
				return 1
			}
			return -1
		} else if first.LowerType != pgtype.Inclusive {
			if firstLower {
				return 1
			}
			return -1
		} else if second.LowerType != pgtype.Inclusive {
			if secondLower {
				return -1
			}
			return 1
		} else {
			return 0
		}
	}

	return result
}

func makeEmptyRange[T any]() pgtype.Range[T] {
	return pgtype.Range[T]{
		LowerType: pgtype.Empty,
		UpperType: pgtype.Empty,
		Valid:     true,
	}
}
