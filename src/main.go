package main

import (
    "kv"
  
    "fmt"
    //"hash/crc32"
    //"io"
)



func main() {

	TestLogAppend()
}


func do(s string) {
    fmt.Println(len(s))
    fmt.Println(s)

}


func TestLogAppend() {
    log := kv.NewLog("d://fortest", 16*1024*1024, false, 1024*16)
    seed := make([]int, 1024*1024)
    fmt.Println(len(seed))
    for i, _ := range seed {
        seed[i] = i+1
        key, value := fmt.Sprintf("%d", i), fmt.Sprintf("%d", seed[i])
        record := kv.NewRecord("D", key, value)
        log.Append(record)

    }

}