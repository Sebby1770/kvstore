package fabric

// Verb wire format. Every field sits at a fixed little-endian offset, so
// the owner executes a verb by reading the header in place -- there is no
// decoding step and nothing is allocated before the memory is touched.
//
//	request (tag 0xFB)                       response (tag 0xFC)
//	 0  1  tag                               0  1  tag
//	 1  1  op                                1  1  op
//	 2  2  region id                         2  1  status
//	 4  4  rkey                              3  1  flags (1 = CAS swapped, 2 = CHASE found)
//	 8  8  request id                        4  4  hops (CHASE)
//	16  8  offset                            8  8  request id
//	24  4  length | CHASE max hops          16  8  CAS/FETCH_ADD previous value | CHASE end offset
//	28  4  CHASE read length                24  …  READ / CHASE data
//	32  4  CHASE link offset
//	36  4  CHASE key offset
//	40  8  CAS expected | FETCH_ADD delta | CHASE key
//	48  8  CAS new | CHASE flags (1 = match key)
//	56  …  WRITE data
const (
	TagRequest  = 0xFB
	TagResponse = 0xFC

	reqHeader  = 56
	respHeader = 24

	// MaxTransfer is the most data one READ, WRITE or CHASE moves; larger
	// transfers are split into several verbs issued concurrently. It keeps
	// every verb inside one KTP datagram.
	MaxTransfer = 1024
)

// Verb opcodes.
const (
	OpRead     = 1
	OpWrite    = 2
	OpCAS      = 3
	OpFetchAdd = 4
	OpChase    = 5
)

// Response status codes.
const (
	stOK = iota
	stBounds
	stAlign
	stKey
	stPerm
	stMalformed
)

var statusErr = [...]error{stOK: nil, stBounds: ErrBounds, stAlign: ErrAlign, stKey: ErrKey, stPerm: ErrPerm, stMalformed: ErrMalformed}

func errStatus(err error) byte {
	switch err {
	case nil:
		return stOK
	case ErrBounds:
		return stBounds
	case ErrAlign:
		return stAlign
	case ErrKey:
		return stKey
	case ErrPerm:
		return stPerm
	}
	return stMalformed
}

// request is a view of a verb request; accessors read the bytes in place.
type request []byte

func (q request) op() byte         { return q[1] }
func (q request) region() uint16   { return le.Uint16(q[2:]) }
func (q request) rkey() uint32     { return le.Uint32(q[4:]) }
func (q request) id() uint64       { return le.Uint64(q[8:]) }
func (q request) offset() uint64   { return le.Uint64(q[16:]) }
func (q request) length() uint32   { return le.Uint32(q[24:]) }
func (q request) readLen() uint32  { return le.Uint32(q[28:]) }
func (q request) linkOff() uint32  { return le.Uint32(q[32:]) }
func (q request) keyOff() uint32   { return le.Uint32(q[36:]) }
func (q request) arg0() uint64     { return le.Uint64(q[40:]) }
func (q request) arg1() uint64     { return le.Uint64(q[48:]) }
func (q request) payload() []byte  { return q[reqHeader:] }
func (q request) setID(id uint64)  { le.PutUint64(q[8:], id) }
func (q request) idempotent() bool { return q.op() == OpRead || q.op() == OpChase }

func newRequest(op byte, rr Remote, off uint64, extra int) request {
	q := make(request, reqHeader+extra)
	q[0], q[1] = TagRequest, op
	le.PutUint16(q[2:], rr.Region)
	le.PutUint32(q[4:], rr.Key)
	le.PutUint64(q[16:], off)
	return q
}

// response is a view of a verb response.
type response []byte

func (r response) status() byte  { return r[2] }
func (r response) flags() byte   { return r[3] }
func (r response) hops() uint32  { return le.Uint32(r[4:]) }
func (r response) id() uint64    { return le.Uint64(r[8:]) }
func (r response) value() uint64 { return le.Uint64(r[16:]) }
func (r response) data() []byte  { return r[respHeader:] }
func (r response) err() error {
	if int(r.status()) < len(statusErr) {
		return statusErr[r.status()]
	}
	return ErrMalformed
}

func newResponse(q request, status byte, dataLen int) response {
	r := make(response, respHeader+dataLen)
	r[0], r[1], r[2] = TagResponse, q.op(), status
	le.PutUint64(r[8:], q.id())
	return r
}

func permFor(op byte) Perm {
	switch op {
	case OpRead, OpChase:
		return PermRead
	case OpWrite:
		return PermWrite
	case OpCAS, OpFetchAdd:
		return PermAtomic
	}
	return 0
}

// execute runs one verb against a region and builds its response. It is
// the whole of the owner's work: bounds and permission checks, then one
// memory operation.
func execute(q request, reg *Region, perm Perm) response {
	need := permFor(q.op())
	if need == 0 {
		return newResponse(q, stMalformed, 0)
	}
	if perm&need == 0 {
		return newResponse(q, stPerm, 0)
	}
	switch q.op() {
	case OpRead:
		n := q.length()
		if n > MaxTransfer {
			return newResponse(q, stMalformed, 0)
		}
		b, err := reg.Bytes(q.offset(), uint64(n))
		if err != nil {
			return newResponse(q, errStatus(err), 0)
		}
		r := newResponse(q, stOK, int(n))
		copy(r.data(), b)
		return r
	case OpWrite:
		p := q.payload()
		if uint32(len(p)) != q.length() {
			return newResponse(q, stMalformed, 0)
		}
		if err := reg.WriteAt(p, q.offset()); err != nil {
			return newResponse(q, errStatus(err), 0)
		}
		return newResponse(q, stOK, 0)
	case OpCAS:
		prev, ok, err := reg.CompareAndSwap64(q.offset(), q.arg0(), q.arg1())
		if err != nil {
			return newResponse(q, errStatus(err), 0)
		}
		r := newResponse(q, stOK, 0)
		le.PutUint64(r[16:], prev)
		if ok {
			r[3] = 1
		}
		return r
	case OpFetchAdd:
		prev, err := reg.FetchAdd64(q.offset(), q.arg0())
		if err != nil {
			return newResponse(q, errStatus(err), 0)
		}
		r := newResponse(q, stOK, 0)
		le.PutUint64(r[16:], prev)
		return r
	case OpChase:
		if q.readLen() > MaxTransfer || q.length() == 0 {
			return newResponse(q, stMalformed, 0)
		}
		res, err := reg.Chase(q.offset(), ChaseSpec{LinkOff: q.linkOff(), KeyOff: q.keyOff(), Key: q.arg0(),
			Match: q.arg1()&1 != 0, MaxHops: q.length(), ReadLen: q.readLen()})
		if err != nil {
			return newResponse(q, errStatus(err), 0)
		}
		r := newResponse(q, stOK, len(res.Data))
		le.PutUint32(r[4:], res.Hops)
		le.PutUint64(r[16:], res.Off)
		if res.Found {
			r[3] = 2
		}
		copy(r.data(), res.Data)
		return r
	}
	return newResponse(q, stMalformed, 0)
}
