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
  		os.Exit(-1)
  	}

  	dir := os.Args[1]
  	cache := kv.NewKVStore(dir, true, 0, 1024*1024)
  	stdin := bufio.NewReader(os.Stdin)
  	helpInfo := `
支持下列命令:
	put "key" "value"
	get "key"
	del "key"
	以及 exit
`
	fmt.Println(helpInfo)

  	for {

  		fmt.Print(">")
  		line, _ := stdin.ReadString(byte('\n'))

  		if check(0, line) == "" {
  			continue
  		}
  		op, k, v, e := parseLine(line)

 		//fmt.Printf("op:%s k:%s v:%s e:%s", op, k, v, e)
  		if e != "" {
  			fmt.Printf("Error: %s\n", e)
  			continue
  		}

  		
  		if op == "put" {
  			cache.Put(k, v)
  			fmt.Println("put done")

  		}else if op == "get" {
  			value := cache.Get(k)
  			if value == ""{
  				fmt.Printf("key %s doesn't exist\n", k)
  			}else{
  				fmt.Println(cache.Get(k))
  			}

  		}else if op == "del" {
  			cache.Del(k)
  			fmt.Println("del done")

  		}else if op == "exit" {
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

	if i==begin+1 {
		errMsg = "string can't be empty"
		return key, errMsg, i
	}
	if i== len(s) || i==begin+1{
		return key, errMsg, i
	}

	return s[begin+1:i], "", i+1

}

// 跳过所有的空格符
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





