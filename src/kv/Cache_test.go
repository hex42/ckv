package kv


import (
	"fmt"
	"testing"
	"math/rand"
	"time"
)



func TestKVStore(t *testing.T) {

	kv := NewKVStore("d://fortest", false, 1024*16, 1024*1024)
	rand.Seed(int64(time.Now().Nanosecond()))
	s := make([]string, 1024*64)
	m := make(map[string]string)
	for i:=0; i<1024*64; i+=1 {
		key, value := fmt.Sprintf("%d", rand.Int()), fmt.Sprintf("%d", rand.Int())
		m[key] = value
		s[i]   = key
	}

	for k, v := range m {
		kv.Put(k, v)
	}

	for k, v := range m {
		i := kv.Get(k)
		if i != v {
			fmt.Printf("error: store %s:%s get %s:%s", k, v, k, i)
		}
	}

	fmt.Printf("kv size %d\n", kv.Size())
	v := []int{0,1, 10, 11, 12, 13, 14, 15, 16,  2, 3, 4, 5, 6, 7, 8, 9}
	bubbleSort(v)
	fmt.Println(v)
	kv.Close()


}

