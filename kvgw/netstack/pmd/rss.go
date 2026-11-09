package pmd

// DefaultRSSKey is the 40-byte Toeplitz key from Microsoft's RSS
// specification, the default on most NICs.
var DefaultRSSKey = []byte{
	0x6d, 0x5a, 0x56, 0xda, 0x25, 0x5b, 0x0e, 0xc2, 0x41, 0x67, 0x25, 0x3d, 0x43, 0xa3, 0x8f, 0xb0,
	0xd0, 0xca, 0x2b, 0xcb, 0xae, 0x7b, 0x30, 0xb4, 0x77, 0xcb, 0x2d, 0xa3, 0x80, 0x30, 0xf2, 0x0c,
	0x6a, 0x42, 0xb7, 0x3b, 0xbe, 0xac, 0x01, 0xfa,
}

// Toeplitz computes the RSS hash of input under key: for every set bit of
// the input, XOR in the 32-bit window of the key starting at that bit. NICs
// hash a flow's addresses and ports this way so that one flow always lands
// on one receive queue (and so one core), keeping its packets in order and
// its state in one cache.
func Toeplitz(key, input []byte) uint32 {
	var h uint32
	if len(key) < 4 {
		return 0
	}
	window := uint32(key[0])<<24 | uint32(key[1])<<16 | uint32(key[2])<<8 | uint32(key[3])
	next := 4
	for _, b := range input {
		for bit := 7; bit >= 0; bit-- {
			if b&(1<<bit) != 0 {
				h ^= window
			}
			// Slide the window one bit left, pulling in the next key bit.
			var in uint32
			if next < len(key) {
				in = uint32(key[next]>>uint(bit)) & 1
			}
			window = window<<1 | in
		}
		next++
	}
	return h
}

// Steer maps a hash to one of n queues, as a NIC's indirection table does.
func Steer(hash uint32, n int) int {
	if n <= 1 {
		return 0
	}
	return int(hash % uint32(n))
}

// FlowHash hashes a KTP packet's flow identity -- the sender's address and
// the destination connection ID (header bytes 2..10) -- so every packet of a
// connection steers to the same queue.
func FlowHash(from string, pkt []byte) uint32 {
	in := make([]byte, 0, len(from)+8)
	in = append(in, from...)
	if len(pkt) >= 10 {
		in = append(in, pkt[2:10]...)
	}
	return Toeplitz(DefaultRSSKey, in)
}
