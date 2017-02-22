package main

import (
    "kv"
  
    "fmt"
    //"hash/crc32"
    //"io"
)

func main() {
   
    r := kv.NewRecord("abc", "def", "d")
    r2:= kv.NewRecord("ab", "def", "d")

    fmt.Println(len(string(r2.ToBytes())))
    f

    

}


func do(s string) {
    fmt.Println(len(s))
    fmt.Println(s)

}
