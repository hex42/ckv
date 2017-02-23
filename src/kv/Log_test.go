package kv

import (
	"fmt"
	"testing"
)

func TestLogAsyncAppend(t *testing.T) {
	log := NewLog("d://fortest", 16*1024*1024, false, 1024*16)
	//seed := make([]int, 1024*1024)
	record := NewRecord("P", "abc", "def")
	log.Append(record)

}


func TestLogSyncAppend(t *testing.T) {

	log := NewLog("d://fortest", 1024*4, true, 0)
	seed := make([]int, 1024*1024)
	fmt.Println(len(seed))
	for i, _ := range seed {
		seed[i] = i+1
		key, value := fmt.Sprintf("%d", i), fmt.Sprintf("%d", seed[i])
		record := NewRecord("P", key, value)
		fmt.Println(key, value)
		log.Append(record)

	}


}

func TestReadLogFile(t *testing.T) {
	log := NewLog("d://fortest", 1024*1024, true, 1024*16)
	records := log.ReadLogFile("2.log")
	for _, record := range records {
		fmt.Printf("%s %s", record.key, record.value)
	}
}


func TestNewRecord(t *testing.T) {

	r := NewRecord("D", "DDD", "def")
	r2 := NewRecord("P", "DDD", "DDD")
	fmt.Println(r.ToBytes())
	fmt.Println(r2.ToBytes())
}

func BeachmarkLogAsyncAppend(t *testing.T) {
	log := NewLog("d://fortest", 16*1024*1024, false, 1024*16)
	seed := make([]int, 1024*1024)
	fmt.Println(len(seed))
	for i, _ := range seed {
		seed[i] = i+1
		key, value := fmt.Sprintf("%d", i), fmt.Sprintf("%d", seed[i])
		record := NewRecord("D", key, value)
		log.Append(record)

	}

}

func BeachmarkLogSyncAppend(t *testing.T) {

	log := NewLog("d://fortest", 16*1024*1024, true, 1024*16)
	seed := make([]int, 1024*1024)
	fmt.Println(len(seed))
	for i, _ := range seed {
		seed[i] = i+1
		key, value := fmt.Sprintf("%d", i), fmt.Sprintf("%d", seed[i])
		record := NewRecord("D", key, value)
		log.Append(record)

	}


}