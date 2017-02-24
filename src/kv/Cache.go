package kv


import (
	"sync"
)


/*
type Cache struct {
	log *Log
	mu  sync.Mutex
	index map[string]offSet
	mem  map[string]
	memSize int64
}

type LRU struct {
}




func (c *Cache) Put(key, value string) bool {
	c.mu.Lock()
	defer c.mu.UnLock()
	record := NewRecord("P", key, value)
	offset := c.log.Append(record)
	if offset.off == -1 {
		return false
	}
	index[key] = offset
	map[key] = value
	memSize += record.Size()
	return true
	

}


func (c *Cache Get(key string) string {
	c.mu.Lock()
	defer c.mu.UnLock()
	value, ok := mem[key]
	if ok {
		return value
	}
	offset := index[key]
        record := c.l.ReadAt(offset)
	if record.value == "" {
	// handle the deleted k v pair
	}
	mem[key] = value
	memSize += record.Size()
        return record.value
	
}


func (c *Cache) Del(key string) bool {
	c.mu.Lock()
	defer c.mu.UnLock()
	record := NewRecord("D", key, "")
	offset := c.log.Append(record)
	index[key] = offset
	map[key] = ""
	mem.Size += record.Size()
}

func (c *Cache) Snapshot() bool {
	

}

func (c *Cache) shrink() bool {
	

}


func NewCache() *Cache {
	
}

*/

