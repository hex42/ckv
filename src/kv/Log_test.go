package kv

import (
	"fmt"
	"testing"
	"math/rand"
	"time"
)



func TestLog(t *testing.T) {
	log := NewLog("/chen/test", 1024*1024, false, 1024)

	records, offsets := log.ReadLogFile("0.log")
	fmt.Printf("reading logfile 0.log read %d record\n", len(records))
	log.Close()

	log = NewLog("/chen/test", 32*1024, false, 1024)
	
	rand.Seed(int64(time.Now().Nanosecond()))

	key, value := make([]string, 1024), make([]string, 1024)
	offsets = make([]*offSet, 1024)
	for i:= 0; i<1024; i+=1 {	
		k := rand.Int()
		v := rand.Int()
		key[i] = fmt.Sprintf("%d", k)
		value[i] = fmt.Sprintf("%d", v)
		record := NewRecord("P", key[i], value[i])
		offset := log.Append(record)
		infoMsg := fmt.Sprintf("recordOffset: %s:%d recordSize:%d checksumSize:%d key: %s v: %s\n", 
						offset.logFile, offset.off, record.Size(), len(record.checksum), record.key, record.value)
		fmt.Printf(infoMsg)
		offsets[i] = offset 
	}

	for i, offset := range offsets {
		record := log.ReadAt(offset)
		if record.value != value[i] || record.key != key[i] {
			errMsg := fmt.Sprintf("Error: Write %s:%s, Got %s:%s", key[i], value[i], record.key, record.value)
			panic(errMsg)
		}
	}


	key, value = make([]string, 1024), make([]string, 1024)
	offsets = make([]*offSet, 1024)
	for i:= 0; i<1024; i+=1 {	
	
		key[i] = fmt.Sprintf("%s", "自是人生长恨水长东")
		value[i] = fmt.Sprintf("%s", "自是人生长恨水长东")
		record := NewRecord("P", key[i], value[i])
		offset := log.Append(record)
		infoMsg := fmt.Sprintf("recordOffset: %s:%d recordSize:%d checksumSize:%d key: %s v: %s\n", 
						offset.logFile, offset.off, record.Size(), len(record.checksum), record.key, record.value)
		fmt.Printf(infoMsg)
		offsets[i] = offset 
	}

	for i, offset := range offsets {
		record := log.ReadAt(offset)
		if record.value != value[i] || record.key != key[i] {
			errMsg := fmt.Sprintf("Error: Write %s:%s, Got %s:%s", key[i], value[i], record.key, record.value)
			panic(errMsg)
		}
	}



	key, value = make([]string, 1024), make([]string, 1024)
	offsets = make([]*offSet, 1024)
	for i:= 0; i<1024; i+=1 {	
		k := rand.Int()
		//v := rand.Int()
		key[i] = fmt.Sprintf("%d", k)
		value[i] = ""
		record := NewRecord("D", key[i], value[i])
		offset := log.Append(record)
		infoMsg := fmt.Sprintf("recordOffset: %s:%d recordSize:%d checksumSize:%d key: %s v: %s\n", 
						offset.logFile, offset.off, record.Size(), len(record.checksum), record.key, record.value)
		fmt.Printf(infoMsg)
		offsets[i] = offset 
	}

	for i, offset := range offsets {
		record := log.ReadAt(offset)
		if record.value != value[i] || record.key != key[i] {
			errMsg := fmt.Sprintf("Error: Write %s:%s, Got %s:%s", key[i], value[i], record.key, record.value)
			panic(errMsg)
		}
	}

	log.Close()
	

}


func TestSort(t *testing.T) {

	rand.Seed(int64(time.Now().Nanosecond()))
	size := rand.Int() % 2048
	if size == 0 {
		size = 2048
	}
	v := make([]int, size)
	for i, _ := range v {
		v[i] = rand.Int()
	}
	bubbleSort(v)
	errMsg := ""
	for i:=0; i<len(v)-1; i+=1 {
		if v[i] > v[i+1] {
			errMsg = fmt.Sprintf("bubbleSortError: %d at position %d but %d at position %d\n", v[i], i, v[i+1], i+1)
			panic(errMsg)

		}
	}

	v = []int{1}
	bubbleSort(v)

	size = rand.Int() % 1048576
	if size == 0 {
		size = 1048576
	}
	v = make([]int, size)
	for i, _ := range v {
		v[i] = rand.Int()
	}

	quickSort(v)

	for i:=0; i<len(v)-1; i+=1 {
		if v[i] > v[i+1] {
			errMsg = fmt.Sprintf("quickSortError: %d at position %d but %d at position %d\n", v[i], i, v[i+1], i+1)
			panic(errMsg)
		}
	}
	
}

