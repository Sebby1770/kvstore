package causal

// FastForward raises this node's delivered vector to at least vc without
// delivering the skipped messages -- for a member that has just installed a
// state snapshot which already includes their effects (a joiner, or a node
// back from a long partition whose missing messages were garbage collected
// everywhere). Buffered messages the snapshot covers are discarded, and any
// that now become deliverable are delivered.
func (n *Node) FastForward(vc VC) {
	for o, s := range vc {
		if o == n.cfg.Self || s <= n.delivered[o] {
			continue
		}
		n.delivered[o] = s
		n.matrix.Observe(n.cfg.Self, o, s)
		if q := n.buffer[o]; q != nil {
			for seq := range q {
				if seq <= s {
					delete(q, seq)
					n.nbuf--
				}
			}
			if len(q) == 0 {
				delete(n.buffer, o)
			}
		}
	}
	n.drain()
}
