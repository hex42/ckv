package main

import (
    "kv"
  
    "fmt"
    //"hash/crc32"
    //"io"
)

func main() {
   
    
    log := kv.NewLog("d://fortest", 4*1024*1024)
    kv := kv.NewRecord("1","2", "P")
    v := make([]int, 1000000)
    for i, _ := range v {
        //k := fmt.Sprintf("%d", i)
        //v := fmt.Sprintf("%d", i+1)
        i+=1
        kv.PutKey("ab")
        kv.PutValue("cd")
        log.Append(kv)
    }



    

}


func do(s string) {
    fmt.Println(len(s))
    fmt.Println(s)

}
