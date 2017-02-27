package kv


import (
	"fmt"
	"testing"
	"math/rand"
	"time"
)



func TestKVStore(t *testing.T) {

	kv := NewKVStore("/chen/test", false, 2*1024, 4*1024*1024)
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
		v2 := kv.Get(k)
		if v2 != v {
			fmt.Printf("Error: store %s:%s get %s:%s", k, v, k, v2)
		}
	}

	fmt.Printf("kv size %d\n", kv.Size())
	kv.Close()

	kv = NewKVStore("/chen/test", false, 128, 32*1024)

	for k, v := range m {
		v2 := kv.Get(k)
		if v2 != v {
			fmt.Printf("Error: store %s:%s get %s:%s", k, v, k, v2)
		}
	}


}


func TestShrink(t *testing.T) {

	kv := NewKVStore("/chen/test", false, 128, 32*1024)
	rand.Seed(int64(time.Now().Nanosecond()))

	s := make([]string, 64)
	m := make(map[string]string)
	for i:=0; i<64; i+=1 {
		key, value := fmt.Sprintf("%d", rand.Int()), fmt.Sprintf("%d", rand.Int())
		m[key] = value
		s[i]   = key
	}

	for k, v := range m {
		kv.Put(k, v)
	}

	for k, v := range m {
		v2 := kv.Get(k)
		if v2 != v {
			fmt.Printf("Error: store %s:%s get %s:%s", k, v, k, v2)
		}
	}

	fmt.Printf("kv size %d\n", kv.Size())
	kv.Close()

	kv = NewKVStore("/chen/test", false, 128, 32*1024)
	kv.Shrink(3)
	for k, v := range m {
		v2 := kv.Get(k)
		if v2 != v {
			fmt.Printf("Error: store %s:%s get %s:%s", k, v, k, v2)
		}
	}

}

