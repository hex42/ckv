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

func (l *LRU) Del()  *LRUItem {
	if l.head == l.tail {
		l.head = l.tail = nil
	}

	et := l.tail
	l.tail = et.previous
	et.previous.next = nil
	et.previous = nil
	return et

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

// kv pair的值不能为空
func (c *KVStore) Put(key, value string) bool {
	if value == "" {
		return false
	}
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
	for  memeSize > memCapacity {
		e := c.lru.Del()
		memSize -= e.record.Size()
	}
	c.lru.Put(e)
	return true

}

// 某个kv对不存在有两种可能，一直以前就没有存过这个kv对，另一种是以前存过但是后来被删除了
func (c *KVStore) Get(key string) string {
	c.mu.Lock()
	defer c.mu.UnLock()
	e, ok := mem[key]
	if ok {
		return e.record.value
	}
	offset, ok := index[key]
	// 从来没有存过
	if !ok {
		return ""
	}
    record := c.l.ReadAt(offset)
	memSize += record.Size()
	for  memeSize > memCapacity {
		e := c.lru.Del()
		memSize -= e.record.Size()
	}
	c.lru.Put(NewLRUItem(record))
	return record.value
	
}


func (c *KVStore) Del(key string) bool {
	c.mu.Lock()
	defer c.mu.UnLock()
	record := NewRecord("D", key, "")
	e := NewLRUItem(record)
	offset := c.log.Append(record)
	c.index[key] = offset
	c.mem[key] = e
	c.memSize += record.Size()
	for  memeSize > memCapacity {
		e := c.lru.Del()
		memSize -= e.record.Size()
	}
	c.lru.Put(e)
	return true
}

func (c *KVStore) Snapshot() bool {

	//找出最小的offset
	

}


func (c *KVStore) build() bool {
	fmt.Println("building kv cache")
	logFiles := c.l.AllLogFiles()
	for logFile := rang logFiles {
		fmt.Println("building from %s\n", logFile)
		records, offsets := c.l.ReadLogFile(logFile)
		for i:=0; i< len(records); i+=1 {
			r := records[i]
			map[r.key] = offsets[i]

		}
	}
	fmt.Printf("building kv cacahe successful")
}

func (c *KVStore) shrink() bool {
	fmt.Printlnn("shrink")
	
	

}


func NewCache(dir string, isLogSync bool, logSyncSize int64, kvMemCapacity int64) *Cache {
	log := NewLog(dir, isLogSync, logSyncSize)
	kvStore := x,x,ddkd
	
	kvStore.build()

	return kvStore
	
}

*/

