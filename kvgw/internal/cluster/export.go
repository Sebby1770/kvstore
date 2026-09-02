package cluster

import "time"

// ExportDoc is the JSON snapshot used by GET /export and POST /import.
// Values are decoded (no TTL envelope leaked). expires_at is unix seconds.
type ExportDoc struct {
	Entries []ExportEntry `json:"entries"`
}

// ExportEntry is one live key in an export document.
type ExportEntry struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Version   uint64 `json:"version"`
	ExpiresAt *int64 `json:"expires_at,omitempty"`
}

// Export is a point-in-time snapshot of every live, non-expired key.
func (c *Cluster) Export() (ExportDoc, []string) {
	rows, errs := c.Scan()
	doc := ExportDoc{Entries: make([]ExportEntry, 0, len(rows))}
	for _, r := range rows {
		doc.Entries = append(doc.Entries, ExportEntry{
			Key:       r.Key,
			Value:     r.Value,
			Version:   r.Version,
			ExpiresAt: r.ExpiresAt,
		})
	}
	return doc, errs
}

// Import puts each entry. Expired rows are skipped. Absolute expiries are
// preserved via PutTTL; versions are not (a new write gets a new version).
func (c *Cluster) Import(doc ExportDoc) (imported, skipped int, errs []string) {
	now := c.Now()
	for _, e := range doc.Entries {
		if e.ExpiresAt != nil && *e.ExpiresAt <= now.Unix() {
			skipped++
			continue
		}
		var err error
		if e.ExpiresAt != nil {
			ttl := time.Unix(*e.ExpiresAt, 0).Sub(now)
			_, err = c.PutTTL([]byte(e.Key), []byte(e.Value), ttl)
		} else {
			_, err = c.Put([]byte(e.Key), []byte(e.Value))
		}
		if err != nil {
			errs = append(errs, e.Key+": "+err.Error())
			continue
		}
		imported++
	}
	return imported, skipped, errs
}
