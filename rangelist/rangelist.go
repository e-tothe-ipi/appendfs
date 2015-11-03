package rangelist

import (
	"fmt"
)


type RangeList struct {
	entries []*RangeListEntry
}

type RangeListEntry struct {
	Min int
	Max int
	Data interface{}
}


func (rl *RangeList) PrintEntries() {
	for _, entry := range rl.entries {
		fmt.Printf("%s\n", entry.String())
	}
}

func (entry RangeListEntry) String() string {
	return fmt.Sprintf("{Min:%d, Max:%d, Data:%p}", entry.Min, entry.Max, entry.Data)
}

func (rl *RangeList) InRange(start int, end int) []*RangeListEntry {
	out := make([]*RangeListEntry, 0)
	if rl.entries == nil {
		return out
	}
	for _, entry := range rl.entries {
		if entry.Min <= end && entry.Max >= start {
			out = append(out, entry)
		}
	}
	return out
}

func (rl *RangeList) Overwrite(newEntry *RangeListEntry) {
	if rl.entries == nil {
		rl.entries = make([]*RangeListEntry, 0)
	}
	oldEntries := make([]*RangeListEntry, len(rl.entries))
	copy(oldEntries, rl.entries)
	rl.entries = rl.entries[:0]
	for _, entry := range oldEntries {
		// Note: there is no case where there is both a split and a delete
		if entry.Min >= newEntry.Min && entry.Max <= newEntry.Max {
			// delete
		} else if entry.Min < newEntry.Min && newEntry.Max < entry.Max {
			// split
			newEntry2 := &RangeListEntry{Min:newEntry.Max + 1,
							Max:entry.Max, Data:entry.Data}
			entry.Max = newEntry.Min - 1
			rl.entries = append(rl.entries, entry)
			rl.entries = append(rl.entries, newEntry2)
		} else if entry.Min < newEntry.Min && entry.Max > newEntry.Min {
			entry.Max = newEntry.Min - 1
			rl.entries = append(rl.entries, entry)
		} else if entry.Max > newEntry.Max && entry.Min < newEntry.Max {
			entry.Min = newEntry.Max + 1
			rl.entries = append(rl.entries, entry)
		} else {
			rl.entries = append(rl.entries, entry)
		}

	}
	pos := 0
	for i, entry := range rl.entries {
		if newEntry.Min > entry.Min {
			pos = i + 1
		}
	}

	rl.entries = append(rl.entries, nil)
	copy(rl.entries[pos+1:], rl.entries[pos:])
	rl.entries[pos] = newEntry

}

func (entry *RangeListEntry) Length() int {
	return entry.Max - entry.Min + 1
}
