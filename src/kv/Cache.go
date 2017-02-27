package kv



import (
	"sync"
	"fmt"
)



type KVStore struct {
	log          *Log
	mu           sync.Mutex
	index        map[string]*offSet
	mem          map[string]*LRUItem
	lru          *LRU
	memSize      int64 //已经缓存的大小
	memCapacity  int64 //允许缓存的大小
}

type LRU struct {
	head *LRUItem 
	tail *LRUItem
}

type LRUItem struct {
	record *Record
	next   *LRUItem
	previous *LRUItem
	
}


func NewLRU() *LRU {
	return &LRU{head:nil, tail:nil}
}


func NewLRUItem(r *Record) *LRUItem {
	return &LRUItem{record:r, next:nil, previous:nil}
}


func (l *LRU) Put(e *LRUItem) {

	if e==nil {
		panic("LRUItem is nil")
	}
	if l.head==nil {
		l.head = e
		l.tail = e
	}
	eh := l.head
	e.next = eh
	eh.previous = e
	l.head = e	
}

// 删除LRU队列的尾部元素
func (l *LRU) Del()  *LRUItem {
	if l.head == l.tail {
		l.head, l.tail = nil, nil
	}

	et := l.tail
	l.tail = et.previous
	et.previous.next = nil
	et.previous = nil
	return et

}


func (l *LRU) Update(e *LRUItem) {

	if e==nil {
		panic("LRUItem is nil")
	}

	if l.head == e || l.head == l.tail  {
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


// e must be an element of lru
func (l *LRU) Delete(e *LRUItem) {
	if l.head == l.tail {
		l.head, l.tail = nil, nil
		e.previous = nil
		e.next = nil
		return
	}

	if e == l.tail {
		l.tail = e.previous
		e.previous.next = nil
		e.previous = nil
		return 
	}

	if e == l.head {
		l.head = e.next
		e.next.previous = nil
		e.next = nil
		return
	}

	e.next.previous = e.previous
	e.previous.next = e.next
	e.previous, e.next = nil, nil

}

// kv pair的value不能为空
func (kv *KVStore) Put(key, value string) bool {
	if value == "" {
		return false
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	record := NewRecord("P", key, value)
	e := NewLRUItem(record)
	offset := kv.log.Append(record)
	if offset.off == -1 {
		return false
	}
	kv.index[key] = offset
	kv.mem[key] = e
	kv.memSize += int64(record.Size())
	for  kv.memSize > kv.memCapacity {
		ele := kv.lru.Del()
		delete(kv.mem, ele.record.key)
		kv.memSize -= ele.record.Size()
	}
	kv.lru.Put(e)
	return true

}

// 某个kv pair不存在有两种可能，一种是没有存过这个kv对，另一种是以前存过但是后来被删除
func (kv *KVStore) Get(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e, ok := kv.mem[key]
	if ok {
		kv.lru.Update(e)
		return e.record.value
	}
	offset, ok := kv.index[key]
	// 从来没有存过
	if !ok {
		return ""
	}
    record := kv.log.ReadAt(offset)
	kv.memSize += int64(record.Size())
	for  kv.memSize > kv.memCapacity {
		ele := kv.lru.Del()
		delete(kv.mem, ele.record.key)
		kv.memSize -= ele.record.Size()
	}
	e = NewLRUItem(record)
	kv.lru.Put( e)
	kv.mem[record.key] = e 
	return record.value
	
}


func (kv *KVStore) Del(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	record := NewRecord("D", key, "")
	e := NewLRUItem(record)
	offset := kv.log.Append(record)
	kv.index[key] = offset
	kv.mem[key] = e
	kv.memSize += int64(record.Size())
	for  kv.memSize > kv.memCapacity {
		ele := kv.lru.Del()
		delete(kv.mem, ele.record.key)
		kv.memSize -= ele.record.Size()
	}
	kv.lru.Put(e)
	return true
}


func (kv *KVStore) Size() int {
	return len(kv.index)
}

func (kv *KVStore) Close() bool {
	kv.log.Close()
	return true
}

func (kv *KVStore) GetVersion() bool {
	
	return true
	
}

// 是否也要构建lru 
func (kv *KVStore) build() bool {
	fmt.Println("building kv cache")
	logFiles := kv.log.AllLogFiles()
	for _, logFile := range logFiles {
		fmt.Printf("building from %s\n", logFile)
		records, offsets := kv.log.ReadLogFile(logFile)
		for i:=0; i< len(records); i+=1 {
			r := records[i]
			kv.index[r.key] = offsets[i]

		}
	}
	fmt.Printf("building kv successful\n")
	return true
}


// 对前n个日志文件进行压缩
func (kv *KVStore) Shrink(n int) {
	
	if !kv.isShrinkable() {
		return 
	}
	
	logFiles := kv.log.AllLogFiles()
	size := len(logFiles)

	for i:=0; i < size && i < n; i+=1 {
		fmt.Printf("shrink file %s", logFiles[i])
		kv.shrinkFile(logFiles[i])

	}
	/*
	logFiles = logFiles[0:size-1]
	for _, logFile := range logFiles {
		kv.shrinkFile(logFile)
	}
	*/


}


func (kv *KVStore) shrinkFile(logFile string) {
	records, offsets := kv.log.ReadLogFile(logFile)
	size := len(records)
	for i:=0; i<size; i+=1 {
		key := records[i].key
		offset := kv.index[key]
		// only the newst record should be append, other just throw away
		if offset.off == offsets[i].off && offset.logFile == offsets[i].logFile {
			newOffset := kv.log.Append(records[i])
			kv.index[key] = newOffset
		}
	}
	kv.log.RemoveLogFile(logFile)
}


func (kv *KVStore) isShrinkable() bool {
	avgSize := float32(kv.memSize) / float32(len(kv.mem))
	avgSize2 := float32(kv.log.Size()) / float32(len(kv.index))
	if avgSize2 > avgSize * 1.8 {
		return true
	}
	return false

}


func NewKVStore(dir string, isLogSync bool, logSyncSize int64, logCapacity int64) *KVStore  {
	log := NewLog(dir, logCapacity, isLogSync, logSyncSize)
	kvStore := &KVStore{log:log, mu:sync.Mutex{}, index:make(map[string]*offSet), 
							mem:make(map[string]*LRUItem), lru:NewLRU(), memSize:0, memCapacity:4*logCapacity }

	kvStore.build()
	return kvStore
	
}





