package auth

import "testing"

func TestLRUEvictsTheLeastRecentlyUsed(t *testing.T) {
	c := newLRU[int](2)
	c.put("a", 1)
	c.put("b", 2)
	if _, ok := c.get("a"); !ok { // a is now the most recent
		t.Fatal("a missing")
	}
	c.put("c", 3) // evicts b, not a
	if _, ok := c.get("b"); ok {
		t.Fatal("b should have been evicted: it was the least recently used")
	}
	if _, ok := c.get("a"); !ok {
		t.Fatal("a was evicted although it was just used")
	}
	c.put("a", 10) // an update also counts as use
	c.put("d", 4)  // evicts c
	if v, ok := c.get("a"); !ok || v != 10 {
		t.Fatalf("a: %v %v", v, ok)
	}
	if _, ok := c.get("c"); ok {
		t.Fatal("c should have been evicted")
	}
	c.remove("a")
	if c.len() != 1 {
		t.Fatalf("len %d", c.len())
	}
}
