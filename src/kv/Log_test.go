package kv

import (
	"fmt"
	"testing"
	"math/rand"
	"time"
)



func TestLog(t *testing.T) {
	log := NewLog("d://fortest", 1024*1024, false, 1024)

	records, offsets := log.ReadLogFile("0.log")
	fmt.Printf("reading logfile 0.log read %d record\n", len(records))
	log.Close()

	log = NewLog("d://fortest", 32*1024, false, 128)
	
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

/*

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

*/


/*
func TestReadLogFile(t *testing.T) {
	log := NewLog("d://fortest", 1024*1024, true, 1024*16)
	records, _ := log.ReadLogFile("3.log")
	for _, record := range records {
		fmt.Printf("%s %s", record.key, record.value)
	}
	log.Close()
}

/*


/*
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

*/