package rangelist

import (


)


type RangeList struct {
	entries []*RangeListEntry
}

type RangeListEntry struct {
	Min int
	Max int
	Data interface{}
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
	pos := 0
	for i, entry := range rl.entries {
		if entry.Min >= newEntry.Min && entry.Max <= newEntry.Max {
			// delete
			rl.entries, rl.entries[len(rl.entries)-1] = append(rl.entries[:i], rl.entries[i+1:]...), nil
		} else if entry.Min < newEntry.Min && entry.Max > newEntry.Min {
			entry.Max = newEntry.Min - 1
		} else if entry.Max > newEntry.Max && entry.Min < newEntry.Max {
			entry.Min = newEntry.Max + 1
		}

		if entry.Min < newEntry.Min {
			pos = i
		}
	}
	rl.entries = append(rl.entries, nil)
	copy(rl.entries[pos+1:], rl.entries[pos:])
	rl.entries[pos] = newEntry
}

func (entry *RangeListEntry) Length() int {
	return entry.Max - entry.Min + 1
}
