package groundtruth

// RowID is a stable identifier for a row in the wide table.
type RowID uint32

// Bitmap represents a RowID set using a simple word-aligned bitset.
// It is intentionally minimal and will be extended as ground-truth logic evolves.
type Bitmap struct {
	words []uint64
}

// NewBitmap returns a bitmap sized to cover [0, size).
func NewBitmap(size int) Bitmap {
	if size <= 0 {
		return Bitmap{}
	}
	return Bitmap{words: make([]uint64, (size+63)/64)}
}

// Set marks the bit for the given RowID.
func (b *Bitmap) Set(id RowID) {
	idx := int(id) >> 6
	if idx < 0 {
		return
	}
	if idx >= len(b.words) {
		b.grow(idx + 1)
	}
	b.words[idx] |= 1 << (uint(id) & 63)
}

// Has reports whether the bit for id is set.
func (b *Bitmap) Has(id RowID) bool {
	idx := int(id) >> 6
	if idx < 0 || idx >= len(b.words) {
		return false
	}
	return (b.words[idx] & (1 << (uint(id) & 63))) != 0
}

// IsEmpty reports whether no bits are set.
func (b Bitmap) IsEmpty() bool {
	for _, w := range b.words {
		if w != 0 {
			return false
		}
	}
	return true
}

// Clone returns a copy of the bitmap.
func (b Bitmap) Clone() Bitmap {
	if len(b.words) == 0 {
		return Bitmap{}
	}
	out := Bitmap{words: make([]uint64, len(b.words))}
	copy(out.words, b.words)
	return out
}

// And returns a new bitmap that is the intersection of b and other.
func (b Bitmap) And(other Bitmap) Bitmap {
	n := min(len(b.words), len(other.words))
	if n == 0 {
		return Bitmap{}
	}
	out := Bitmap{words: make([]uint64, n)}
	for i := 0; i < n; i++ {
		out.words[i] = b.words[i] & other.words[i]
	}
	return out
}

// Or returns a new bitmap that is the union of b and other.
func (b Bitmap) Or(other Bitmap) Bitmap {
	n := max(len(b.words), len(other.words))
	if n == 0 {
		return Bitmap{}
	}
	out := Bitmap{words: make([]uint64, n)}
	copy(out.words, b.words)
	for i := 0; i < len(other.words); i++ {
		out.words[i] |= other.words[i]
	}
	return out
}

// Not returns a new bitmap that inverts bits within [0, size).
func (b Bitmap) Not(size int) Bitmap {
	if size <= 0 {
		return Bitmap{}
	}
	n := (size + 63) / 64
	out := Bitmap{words: make([]uint64, n)}
	for i := 0; i < n; i++ {
		var w uint64
		if i < len(b.words) {
			w = b.words[i]
		}
		out.words[i] = ^w
	}
	// Clear trailing bits beyond size.
	lastBits := size & 63
	if lastBits != 0 {
		mask := uint64(1)<<uint(lastBits) - 1
		out.words[n-1] &= mask
	}
	return out
}

// Sub returns a new bitmap that is the set difference b \ other.
func (b Bitmap) Sub(other Bitmap) Bitmap {
	n := min(len(b.words), len(other.words))
	out := b.Clone()
	for i := 0; i < n; i++ {
		out.words[i] &^= other.words[i]
	}
	return out
}

// AndWith updates b to be the intersection of b and other.
func (b *Bitmap) AndWith(other Bitmap) {
	n := min(len(b.words), len(other.words))
	for i := 0; i < n; i++ {
		b.words[i] &= other.words[i]
	}
	for i := n; i < len(b.words); i++ {
		b.words[i] = 0
	}
}

// OrWith updates b to be the union of b and other.
func (b *Bitmap) OrWith(other Bitmap) {
	if len(other.words) == 0 {
		return
	}
	if len(b.words) < len(other.words) {
		b.grow(len(other.words))
	}
	for i := 0; i < len(other.words); i++ {
		b.words[i] |= other.words[i]
	}
}

// Count returns the number of set bits.
func (b Bitmap) Count() int {
	total := 0
	for _, w := range b.words {
		total += popcount64(w)
	}
	return total
}

func (b *Bitmap) grow(size int) {
	if size <= len(b.words) {
		return
	}
	words := make([]uint64, size)
	copy(words, b.words)
	b.words = words
}

func popcount64(v uint64) int {
	// Hacker's Delight popcount.
	v = v - ((v >> 1) & 0x5555555555555555)
	v = (v & 0x3333333333333333) + ((v >> 2) & 0x3333333333333333)
	return int((((v + (v >> 4)) & 0x0F0F0F0F0F0F0F0F) * 0x0101010101010101) >> 56)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
