package appendfs

func setBit(attr *uint32, mask uint32, field uint32) {
	*attr &= ^mask
	*attr |= (mask & field)
}

func getBit(attr *uint32, mask uint32) bool {
	return *attr & mask > 0
}

