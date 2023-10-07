package timeutil

import (
	"fmt"
	"sort"
	"time"
)

type TemporalCollection struct {
	_milestonesCache timeSlice
	contents         map[time.Time]interface{}
}

func NewTemporalCollection() *TemporalCollection {
	return &TemporalCollection{contents: make(map[time.Time]interface{})}
}

func (tc *TemporalCollection) Put(at time.Time, item interface{}) {
	tc.contents[at] = item
	tc.clearMilestoneCache()
}

func (tc *TemporalCollection) Get(when time.Time) (interface{}, error) {
	for _, date := range tc.milestones() {
		thisDate := date
		if thisDate.Before(when) || thisDate.Equal(when) {
			return tc.contents[thisDate], nil
		}
	}
	return nil, fmt.Errorf("no records that early")
}

func (tc *TemporalCollection) milestones() timeSlice {
	if tc._milestonesCache == nil {
		tc.calculateMilestones()
	}
	return tc._milestonesCache
}

func (tc *TemporalCollection) calculateMilestones() {
	tc._milestonesCache = make(timeSlice, 0, len(tc.contents))
	for k := range tc.contents {
		tc._milestonesCache = append(tc._milestonesCache, k)
	}
	sort.Sort(sort.Reverse(tc._milestonesCache))
}

func (tc *TemporalCollection) clearMilestoneCache() {
	tc._milestonesCache = nil
}

type timeSlice []time.Time

func (ts timeSlice) Len() int {
	return len(ts)
}

func (ts timeSlice) Less(i, j int) bool {
	return ts[i].Before(ts[j])
}

func (ts timeSlice) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}
