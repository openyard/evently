package timeutil

import (
	"bytes"
	"fmt"
	"sort"
	"time"
)

var (
	past   = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
	future = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)

	EmptyTimeRange = &TimeRange{
		start: time.Date(2000, 4, 1, 0, 0, 0, 0, time.UTC),
		end:   time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
	}
)

// TimeRange provides time range helper functions
type TimeRange struct {
	start time.Time
	end   time.Time
}

// NewTimeRange returns a new time range with given start and end parameters
func NewTimeRange(start, end time.Time) *TimeRange {
	return &TimeRange{
		start: start,
		end:   end,
	}
}

// UpTo returns a time range with given end
func UpTo(end time.Time) *TimeRange {
	return &TimeRange{start: past, end: end}
}

// StartingOn returns a time range with given start
func StartingOn(start time.Time) *TimeRange {
	return &TimeRange{start: start, end: future}
}

func (tr *TimeRange) Start() time.Time {
	return tr.start
}

func (tr *TimeRange) End() time.Time {
	return tr.end
}

func (tr *TimeRange) Includes(timepoint time.Time) bool {
	return !timepoint.Before(tr.start) && !timepoint.After(tr.end)
}

func (tr *TimeRange) IncludesRange(arg *TimeRange) bool {
	return tr.Includes(arg.start) || tr.Includes(arg.end)
}

func (tr *TimeRange) Overlaps(arg *TimeRange) bool {
	return arg.Includes(tr.start) || arg.Includes(tr.end) || tr.IncludesRange(arg)
}

func (tr *TimeRange) Equals(arg interface{}) bool {
	switch arg.(type) {
	case TimeRange:
		other := arg.(*TimeRange)
		return tr.start.Equal(other.start) && tr.end.Equal(other.end)
	default:
		return false
	}
}

func (tr *TimeRange) Gap(arg *TimeRange) *TimeRange {
	if tr.Overlaps(arg) {
		return EmptyTimeRange
	}
	var lower, higher *TimeRange
	if tr.CompareTo(arg) < 0 {
		lower = tr
		higher = arg
	} else {
		lower = arg
		higher = tr
	}
	return NewTimeRange(lower.end.AddDate(0, 0, 1), higher.start.AddDate(0, 0, -1))
}

func (tr *TimeRange) CompareTo(arg interface{}) int {
	other := arg.(*TimeRange)
	if !tr.start.Equal(other.start) {
		start, _ := tr.start.MarshalBinary()
		ostart, _ := other.start.MarshalBinary()
		return bytes.Compare(start, ostart)
	}
	start, _ := tr.end.MarshalBinary()
	ostart, _ := other.end.MarshalBinary()
	return bytes.Compare(start, ostart)
}

func (tr *TimeRange) Abuts(arg *TimeRange) bool {
	return !tr.Overlaps(arg) && tr.Gap(arg).isEmpty()
}

func (tr *TimeRange) PartitionedBy(args timeRangeSlice) bool {
	sort.Sort(args)
	if !tr.IsContigous(args) {
		return false
	}
	combination, _ := tr.Combination(args)
	return tr.Equals(combination)
}

func (tr *TimeRange) Combination(args timeRangeSlice) (*TimeRange, error) {
	sort.Sort(args)
	if !tr.IsContigous(args) {
		return nil, fmt.Errorf("unable to combine date reanges")
	}
	return NewTimeRange(args[0].start, args[len(args)-1].end), nil
}

func (tr *TimeRange) IsContigous(args timeRangeSlice) bool {
	sort.Sort(args)
	for i := 0; i < len(args)-1; i++ {
		if !args[i].Abuts(args[i+1]) {
			return false
		}
	}
	return true
}

type timeRangeSlice []*TimeRange

func (p timeRangeSlice) Len() int {
	return len(p)
}

func (p timeRangeSlice) Less(i, j int) bool {
	return p[i].start.Before(p[j].start)
}

func (p timeRangeSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (tr *TimeRange) String() string {
	if tr.isEmpty() {
		return "empty date range"
	}
	return fmt.Sprintf("%s - %s", tr.start, tr.end)
}

func (tr *TimeRange) isEmpty() bool {
	return tr.start.After(tr.end)
}
