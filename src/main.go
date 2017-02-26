package main

import (
//    "kv"
    "bufio"
  	"os"
    "fmt"
    //"hash/crc32"
    //"io"
)



func main() {

   for i, v := range os.Args {
   		fmt.Println(i, v)
	}

    stdin := bufio.NewReader(os.Stdin)
    //stdout := bufio.NewWriter(os.Stdout)
    for {
        fmt.Println(">")
        c, _ := stdin.ReadString(byte('\n'))
        fmt.Println(c)

    }

    
}

/*
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
*/
