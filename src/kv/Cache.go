package kv

/*

import (
	"sync"
)



type KVStore struct {
	log *Log
	mu  sync.Mutex
	index map[string]*offSet
	mem  map[string]*LRUItem
	lru *LRU
	memSize int64 //已经缓存的大小
	memCapacity int64 //允许缓存的大小
}

type LRU struct {
	head *LRUtem 
	tail *LRUItem
}

type LRUItem struct {
	record *Record
	next   *LRUItem
	previous *LRUItem
	
}


func NewLRU() *LRU {
	retrun &LRU{head:nil, tail:nil}
}


func NewLRUItem(r *Record) *LRUItem {
	retrun &LRUItem{record:r, next:nil, previous:nil}
}


func (l *LRU) Put(e *LRUItem) {
	if l.head==nil {
		l.head = e
		l.tail = e
	}
	eh := l.head
	e.next = eh
	eh.previous = e
	l.head = e	
}

func (l *LRU) Del()  {
	if l.head == l.tail {
		l.head = l.tail = nil
	}

	et := l.tail
	l.tail = et.previous
	et.previous.next = nil
	et.previous = nil

}


func (l *LRU) Update(e *LRUItem) {
	if l.head == l.tail || l.head == e {
		return
	}


	if e == l.tail {
		l.tail = e.previous
		e.previous.next = nil
		e.previous = nil
		eh := l.head
		l.head = e
		e.next = eh
		eh.previous = e
		return
	}

	e.previous.next = e.next
	e.next.previous = e.previous
	e.previous = nil
	eh := l.head
	e.next = eh
	eh.previous = e
	l.head = e
	return 
	
}


func (c *KVStore) Put(key, value string) bool {
	c.mu.Lock()
	defer c.mu.UnLock()
	record := NewRecord("P", key, value)
	e := NewLRUItem(record)
	offset := c.log.Append(record)
	if offset.off == -1 {
		return false
	}
	index[key] = offset
	mem[key] = e
	memSize += record.Size()
	if memSize > memCapacity {
		c.lru.Del()
	}
	c.lru.Put(e)
	return true

}


func (c *KVStore) Get(key string) string {
	c.mu.Lock()
	defer c.mu.UnLock()
	e, ok := mem[key]
	if ok {
		return e.record.value
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


func (c *KVStore) Del(key string) bool {
	c.mu.Lock()
	defer c.mu.UnLock()
	record := NewRecord("D", key, "")
	offset := c.log.Append(record)
	index[key] = offset
	map[key] = ""
	mem.Size += record.Size()
}

func (c *KVStore) Snapshot() bool {
	

}

func (c *KVStore) build() bool {
	
	
}

func (c *KVStore) shrink() bool {
	

}


func NewCache() *Cache {
	
}

*/

