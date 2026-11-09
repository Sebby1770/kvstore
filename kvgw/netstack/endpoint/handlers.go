package endpoint

// Tagged handlers let subsystems share one endpoint's datagrams by their
// first byte, and run where the packet is processed instead of on a
// goroutine of their own.
//
// A DatagramHandler sees connection datagrams (KTP DATAGRAM frames) whose
// first byte is its tag. It runs inside the endpoint's packet path, under
// the endpoint lock, so it must be quick and must not call back into this
// Conn or Endpoint; whatever it returns is sent straight back on the same
// connection. That is what lets package fabric serve one-sided memory verbs
// the way an RDMA NIC does: on arrival, against registered memory, without
// waking any application goroutine.
//
// A RawHandler sees connectionless Raw datagrams whose first byte is its
// tag (SWIM's own messages start with small type numbers and keep using
// RecvRaw). It runs on the endpoint's reader goroutine, without the lock.
// Datagrams with no handler for their tag queue as before.

// DatagramHandler handles one tagged connection datagram; a non-nil reply
// is sent back on c.
type DatagramHandler func(c *Conn, p []byte) (reply []byte)

// RawHandler handles one tagged Raw datagram from a peer address.
type RawHandler func(from string, p []byte)

// HandleDatagrams routes connection datagrams starting with tag to h (nil
// removes the route).
func (e *Endpoint) HandleDatagrams(tag byte, h DatagramHandler) {
	e.hmu.Lock()
	e.dgHandlers[tag] = h
	e.hmu.Unlock()
}

// HandleRaw routes Raw datagrams starting with tag to h (nil removes it).
// Tags below 0x20 are reserved for SWIM.
func (e *Endpoint) HandleRaw(tag byte, h RawHandler) {
	e.hmu.Lock()
	e.rawHandler[tag] = h
	e.hmu.Unlock()
}

func (e *Endpoint) rawHandlerFor(tag byte) RawHandler {
	e.hmu.RLock()
	defer e.hmu.RUnlock()
	return e.rawHandler[tag]
}

func (e *Endpoint) datagramHandlerFor(tag byte) DatagramHandler {
	e.hmu.RLock()
	defer e.hmu.RUnlock()
	return e.dgHandlers[tag]
}

// dispatchDatagramsLocked drains c's received datagrams, handing tagged ones
// to their handlers and keeping the rest for ReceiveDatagram. It reports
// whether a handler queued a reply.
func (e *Endpoint) dispatchDatagramsLocked(c *Conn) (replied bool) {
	for {
		d, ok := c.kc.RecvDatagram()
		if !ok {
			return replied
		}
		var h DatagramHandler
		if len(d) > 0 {
			h = e.datagramHandlerFor(d[0])
		}
		if h == nil {
			if len(c.dgq) >= 1024 {
				c.dgq = c.dgq[1:]
			}
			c.dgq = append(c.dgq, d)
			continue
		}
		if reply := h(c, d); reply != nil {
			if err := c.kc.SendDatagram(reply); err == nil {
				replied = true
			}
		}
	}
}

// SendDatagramLocked is SendDatagram for use inside a DatagramHandler (which
// already holds the endpoint lock): it queues p without flushing; the
// endpoint flushes after the handler returns.
func (c *Conn) SendDatagramLocked(p []byte) error { return c.kc.SendDatagram(p) }
