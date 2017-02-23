package main

import (
    //"kv"
  
    "fmt"
    //"hash/crc32"
    //"io"
)



func main() {

	v := make(map[string]string)
	v["abc"] = "def"
	d := v["abc"]

	fmt.Println(d)
    d, ok := v["abc"]
    fmt.Println(ok)
}


func do(s string) {
    fmt.Println(len(s))
    fmt.Println(s)

}
