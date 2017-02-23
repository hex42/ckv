package kv


type Cache struct {
	log *Log
	mu  syn.Mutex
	index map[string]offSet
	mem  map[string]
	memSize int64
}


func (c *Cache) Put(key, value string) bool {
	mu.Lock()
	defer mu.UnLock()

}


func (c *Cache Get(key string) string {
}


func (c *Cache) Del(key string) bool {
}


func NewCache() bool {
}


	
