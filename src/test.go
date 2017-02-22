package main

import (
    "hash/crc32"
    "fmt"
)

func main() {
   
   s := "abc"
   checksum := crc32.ChecksumIEEE([]byte(s))
   fmt.Println(checksum)
   fmt.Println("cde"+"cd")
    

}


func do(s string) {
    fmt.Println(len(s))
    fmt.Println(s)

}
