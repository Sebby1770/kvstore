//! A sorted set of disjoint half-open intervals [lo, hi). Streams use it for
//! acknowledged and to-be-retransmitted byte ranges; the ACK tracker uses it
//! for received packet numbers.

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangeSet(Vec<(u64, u64)>);

impl RangeSet {
    pub fn new() -> RangeSet {
        RangeSet(Vec::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn first(&self) -> Option<(u64, u64)> {
        self.0.first().copied()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &(u64, u64)> {
        self.0.iter()
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Drops the lowest range.
    pub fn pop_first(&mut self) -> Option<(u64, u64)> {
        if self.0.is_empty() {
            None
        } else {
            Some(self.0.remove(0))
        }
    }

    /// Inserts [lo, hi), merging the neighbours it touches.
    pub fn add(&mut self, mut lo: u64, mut hi: u64) {
        if lo >= hi {
            return;
        }
        let r = &mut self.0;
        let i = r.iter().position(|x| x.1 >= lo).unwrap_or(r.len());
        let mut j = i;
        while j < r.len() && r[j].0 <= hi {
            lo = lo.min(r[j].0);
            hi = hi.max(r[j].1);
            j += 1;
        }
        r.splice(i..j, std::iter::once((lo, hi)));
    }

    /// Deletes [lo, hi).
    pub fn remove(&mut self, lo: u64, hi: u64) {
        if lo >= hi {
            return;
        }
        let mut out = Vec::with_capacity(self.0.len() + 1);
        for &(a, b) in &self.0 {
            if b <= lo || a >= hi {
                out.push((a, b));
                continue;
            }
            if a < lo {
                out.push((a, lo));
            }
            if b > hi {
                out.push((hi, b));
            }
        }
        self.0 = out;
    }

    pub fn contains(&self, v: u64) -> bool {
        for &(a, b) in &self.0 {
            if v < a {
                return false;
            }
            if v < b {
                return true;
            }
        }
        false
    }

    /// The parts of [lo, hi) not in the set.
    pub fn gaps(&self, lo: u64, hi: u64) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        let mut cur = lo;
        for &(a, b) in &self.0 {
            if b <= cur {
                continue;
            }
            if a >= hi {
                break;
            }
            if a > cur {
                out.push((cur, a.min(hi)));
            }
            cur = cur.max(b);
            if cur >= hi {
                break;
            }
        }
        if cur < hi {
            out.push((cur, hi));
        }
        out
    }

    /// Drops everything below v.
    pub fn trim_below(&mut self, v: u64) {
        self.0.retain(|x| x.1 > v);
        if let Some(first) = self.0.first_mut() {
            first.0 = first.0.max(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RangeSet;

    #[test]
    fn add_merges_and_remove_splits() {
        let mut s = RangeSet::new();
        s.add(10, 20);
        s.add(30, 40);
        s.add(20, 30); // touches both: one range
        assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![(10, 40)]);
        s.remove(15, 25);
        assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![(10, 15), (25, 40)]);
        assert!(s.contains(10) && !s.contains(15) && s.contains(39) && !s.contains(40));
        assert_eq!(s.gaps(0, 50), vec![(0, 10), (15, 25), (40, 50)]);
        s.trim_below(30);
        assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![(30, 40)]);
        s.add(5, 5); // empty: no-op
        assert_eq!(s.len(), 1);
    }
}
