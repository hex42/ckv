package main

import (
	"os"
    "fmt"
    "kv"
    "bufio"
)

func main() {
   
  	if len(os.Args) != 2 {
  		fmt.Println("error only need the dir for kv")
  	}

  	cache := kv.NewKVStore("/chen/test", false, 1024, 1024*1024)
  	stdin := bufio.NewReader(os.Stdin)

  	for {
  		fmt.Print(">")
  		line, _ := stdin.ReadString(byte('\n'))
  		op, k, v, e := parseLine(line)

  		if e != "" {
  			fmt.Printf("%s\n", e)
  			continue
  		}
  		if op == "put" {
  			cache.Put(k, v)
  		}
  		if op == 'get' {
  			fmt.Println(cache.Get(k))
  		}
  		if op == 'del' {
  			cache.Del(k)
  		}

  		if op == 'exit' {
  			cache.Close()
  			return
  		}

  	}

}



func parseLine(line string) (string, string, string, string) {
	command, key, value, err := "", "", "", ""
	i:=0
	size := len(line)
	for i < size {
		if line[i] == "\t" || line[i] == " "{
			i+=1
		}else{
			break
		}
	}

	if i+3 > size {
		return op, key, value, "expecting put, del, get or exit command"
	}
	command := line[i:i+3]
	if command != put && command != "get" && command != "del" {
		if i+4 > size {
			return op, key, value, "expecting put, del, get or exit command"
		}
		command := line[i:i+4]
		if command != "exit" {
			return op, key, value, "expecting put, del, get or exit command"
		}
	}

	i+=3
	if command == "exit"{
		i = check(i, line)
		return command, key, value, err
	}

	if command == "del" || command == "get" {
		key, i = readString(i, line)
		i = check(i, line)

	}

	if command == "put" {
		key, i = readString(i,line )
		value, i = readString(i, line)
		i = check(i, line)

	}

	if i!= -1 {
		return commnd, key, value, err
	}

	return commadn, key, value, "wrong"


}


def readString(i int, s string) string {
	i:=0
	for i < len(s) {
		if line[i] == "\t" || line[i] == " "{
			i+=1
		}else{
			break
		}
	}
	if s[i] != "\"" {
		return -1
	}
	i+= 1
	for i < len(s) {
		if s[i] != "\"" {
			i+=1
		}else{
			break
		}
	}
	return s[begin:i]

}


def check(i int, s string) bool {

	for i < len(s) {
		if s[i] != " " || s[i] != "\n" || s[i] != "\t" {
			return false
		} 
	}
	return true
}





