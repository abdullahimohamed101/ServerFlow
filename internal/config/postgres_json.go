package config

import "encoding/json"

// MarshalJSON redacts the DSN (its password is inside it), so encoding a whole Config as JSON, for example
// to log it, cannot leak the database password. RedisConfig does the same for its secrets.
func (p PostgresConfig) MarshalJSON() ([]byte, error) {
	type plain PostgresConfig // no methods, so no recursion
	q := plain(p)
	if q.DSN != "" {
		q.DSN = "<redacted>"
	}
	return json.Marshal(q)
}
