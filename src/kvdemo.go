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

  		//fmt.Printf("op:%s k:%s v:%s e:%s", op, k, v, e)
  		if e != "" {
  			fmt.Printf("%s\n", e)
  			continue
  		}

  		
  		if op == "put" {
  			cache.Put(k, v)
  			fmt.Println("put done")

  		}

  		if op == "get" {
  			fmt.Println(cache.Get(k))
  		}

  		if op == "del" {
  			cache.Del(k)
  			fmt.Println("del done")
  		}

  		if op == "exit" {
  			cache.Close()
  			return
  		}
  		

  	}

}



func parseLine(line string) (string, string, string, string) {

	i := 0
	command, key, value := "", "", "" 

	command, errMsg, i:= readCommand(i, line)

	if errMsg != "" {
		return command, key, value, errMsg
	}


	if command == "exit"{
		
		errMsg = check(i, line)
		return command, key, value, errMsg
	}

	if command == "del" || command == "get" {
		key, errMsg, i = readString(i, line)
		if errMsg != "" {
			return command, key, value, errMsg
		}
		errMsg = check(i, line)
		return command, key, value, errMsg



	}

	if command == "put" {
		key,  errMsg, i = readString(i,line )
		if errMsg != "" {
			return command, key, value, errMsg
		}
		value, errMsg, i = readString(i, line)
		if errMsg != "" {
			return command, key, value, errMsg
		}
		errMsg = check(i, line)

		if errMsg != "" {
			return command, key, value, errMsg
		}

	}

	return command, key, value, errMsg

}



func readCommand(i int, s string) (string, string, int) {
	command := ""
	i = skip(i, s)
	errMsg := "expecting put, del, get or exit command"
	if i+3 > len(s) {
		return command, errMsg, i
	}
	command = s[i:i+3]
	if command != "put" && command != "get" && command != "del" {
		if i+4 > len(s) {
			return command, errMsg, i
		}
		command = s[i:i+4]
		if command != "exit" {
			return command, errMsg, i
		}
	}

	return command, "", i+len(command)

}

func readString(i int, s string) (string, string, int) {
	begin := skip(i, s)
	key := ""
	errMsg := "expecting symbol \""
	if begin == len(s) {
		return key, errMsg, begin
	}
	if string(s[begin]) != "\"" {
		return key, errMsg,begin
	}
	i = begin+1
	for i < len(s) {
		if string(s[i]) != "\"" {
			i+=1
		}else{
			break
		}
	}

	if i== len(s) {
		return key, errMsg, begin
	}

	return s[begin+1:i], "", i+1

}


func skip(i int, s string) int {
	size := len(s)
	for i < size {
		if string(s[i]) == "\t" || string(s[i]) == " "{
			i+=1
		}else{
			break
		}
	}
	return i

}



func check(i int, s string) string {
	if i >= len(s) {
		return "expecting a new line symbol"
	}
	for i < len(s) {
		e := string(s[i])
		if e != " " && e != "\n" && e != "\t" {
			return  fmt.Sprintf("not expecting %s", e)
		} 
		i+=1
	}
	return ""
}





