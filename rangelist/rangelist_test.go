package rangelist

import (
	"testing"

)


func TestOneItem(t *testing.T) {
	rl := &RangeList{}
	rl.Overwrite(&RangeListEntry{Min:0,Max:100})
	entries := rl.InRange(0, 100)
	if len(entries) != 1 {
		t.Fatalf("Should have 1 entry")
	}
	entries = rl.InRange(0, 200)
	if len(entries) != 1 {
		t.Fatalf("Should have 1 entry")
	}
	entries = rl.InRange(0, 0)
	if len(entries) != 1 {
		t.Fatalf("Should have 1 entry")
	}
	entries = rl.InRange(101, 200)
	if len(entries) != 0 {
		t.Fatalf("Should have no entry")
	}
}

func TestOverwriteBasic(t *testing.T) {
	rl := &RangeList{}
	rl.Overwrite(&RangeListEntry{Min:0,Max:100})
	rl.Overwrite(&RangeListEntry{Min:0,Max:100})
	entries := rl.InRange(0, 100)
	if len(entries) != 1 {
		t.Fatalf("Should have 1 entry")
	}
	rl.Overwrite(&RangeListEntry{Min:50,Max:100})
	entries = rl.InRange(0, 100)
	if entries[0].Max != 49 {
		t.Fatalf("Should have overwritten up to 49, was %d", entries[0].Max)
	}
	if len(entries) != 2 {
		t.Fatalf("Should have 2 entries now")
	}
	rl.Overwrite(&RangeListEntry{Min:22, Max:22})
	entries = rl.InRange(0, 100)
	if len(entries) != 4 {
		t.Fatalf("Should have 4 entries")
	}
	if entries[0].Max != 21 {
		t.Fatalf("First entry should have Max 21")
	}
	if entries[1].Min != 22 || entries[1].Max != 22 {
		t.Fatalf("Second entry should be {22,22}")
	}
	if entries[2].Min != 23 {
		t.Fatalf("Third entry should have Min 23")
	}
	if entries[2].Max != 49 {
		t.Fatalf("Third entry should have Max 49, was %d", entries[2].Max)
	}
	if entries[3].Min != 50 || entries[3].Max != 100 {
		t.Fatalf("Fourth entry should be {50, 100}")
	}
}

func TestOverwriteGap(t *testing.T) {
	rl := &RangeList{}
	rl.Overwrite(&RangeListEntry{Min:1,Max:100})
	rl.Overwrite(&RangeListEntry{Min:201,Max:300})
	rl.Overwrite(&RangeListEntry{Min:301,Max:400})
	rl.Overwrite(&RangeListEntry{Min:101,Max:200})

	if len(rl.entries) != 4 {
		t.Fatalf("Should have 4 entries")
	}

	if rl.entries[1].Min != 101 || rl.entries[1].Max != 200 {
		t.Fatalf("Entry 1 is wrong")
	}
}

func TestOverwriteMiddle(t *testing.T) {
	rl := &RangeList{}
	rl.Overwrite(&RangeListEntry{Min:1,Max:100})
	rl.Overwrite(&RangeListEntry{Min:101,Max:200})
	rl.Overwrite(&RangeListEntry{Min:201,Max:300})
	rl.Overwrite(&RangeListEntry{Min:101,Max:200})

	if len(rl.entries) != 3 {
		t.Fatalf("Should have 3 entries")
	}
}

func TestOverwriteMultiEnd(t *testing.T) {
	rl := &RangeList{}
	rl.Overwrite(&RangeListEntry{Min:1,Max:100})
	rl.Overwrite(&RangeListEntry{Min:101,Max:200})
	rl.Overwrite(&RangeListEntry{Min:201,Max:300})
	rl.Overwrite(&RangeListEntry{Min:301,Max:400})
	rl.Overwrite(&RangeListEntry{Min:100,Max:400})

	if len(rl.entries) != 2 {
		t.Fatalf("Should have 2 entries")
	}
}

func TestOverwriteMultiStart(t *testing.T) {
	rl := &RangeList{}
	rl.Overwrite(&RangeListEntry{Min:1,Max:100})
	rl.Overwrite(&RangeListEntry{Min:101,Max:200})
	rl.Overwrite(&RangeListEntry{Min:201,Max:300})
	rl.Overwrite(&RangeListEntry{Min:301,Max:400})
	rl.Overwrite(&RangeListEntry{Min:1,Max:299})

	if len(rl.entries) != 3 {
		t.Fatalf("Should have 3 entries")
	}
	if rl.entries[0].Min != 1 || rl.entries[0].Max != 299 {
		t.Fatalf("Entry 0 is wrong")
	}
	if rl.entries[1].Min != 300 || rl.entries[1].Max != 300 {
		t.Fatalf("Entry 1 is wrong")
	}
	if rl.entries[2].Min != 301 || rl.entries[2].Max != 400 {
		t.Fatalf("Entry 2 is wrong")
	}
}

