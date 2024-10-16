package es

import (
	"sort"

	"github.com/openyard/evently/event"
)

func Merge(changes []Change) []Change {
	res := make([]Change, 0)
	streams := make(map[string][]Change)
	for _, change := range changes {
		streams[change.stream] = append(streams[change.stream], change)
	}
	mergedStreams := make(map[string]Change)
	for stream, change := range streams {
		mergedStreams[stream] = mergeChanges(change...)
	}
	for _, v := range mergedStreams {
		res = append(res, v)
	}
	return res

}

func mergeChanges(s ...Change) Change {
	res := NewChange(s[0].stream, s[0].expectedVersion, make([]*event.Event, 0)...)
	for _, v := range s {
		res.events = append(res.events, v.events...)
		if res.expectedVersion > v.expectedVersion {
			res.expectedVersion = v.expectedVersion
		}
	}
	sort.Slice(res.events[:], func(i, j int) bool {
		return res.events[i].OccurredAt().Before(res.events[j].OccurredAt())
	})
	return res
}
