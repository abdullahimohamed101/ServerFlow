package auth

import "container/list"

// lru is a size-bounded least-recently-used map. It is not safe for concurrent use; the
// Authenticator guards it with its mutex.
type lru[V any] struct {
	max   int
	order *list.List // front = most recently used
	items map[string]*list.Element
}

type lruItem[V any] struct {
	key string
	val V
}

func newLRU[V any](max int) *lru[V] {
	return &lru[V]{max: max, order: list.New(), items: make(map[string]*list.Element, min(max, 1024))}
}

func (c *lru[V]) get(key string) (V, bool) {
	if el, ok := c.items[key]; ok {
		c.order.MoveToFront(el)
		return el.Value.(*lruItem[V]).val, true
	}
	var zero V
	return zero, false
}

func (c *lru[V]) put(key string, val V) {
	if el, ok := c.items[key]; ok {
		el.Value.(*lruItem[V]).val = val
		c.order.MoveToFront(el)
		return
	}
	c.items[key] = c.order.PushFront(&lruItem[V]{key: key, val: val})
	for c.order.Len() > c.max {
		last := c.order.Back()
		delete(c.items, last.Value.(*lruItem[V]).key)
		c.order.Remove(last)
	}
}

func (c *lru[V]) remove(key string) {
	if el, ok := c.items[key]; ok {
		delete(c.items, key)
		c.order.Remove(el)
	}
}

func (c *lru[V]) len() int { return c.order.Len() }
