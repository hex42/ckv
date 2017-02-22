package kv

import (
    "os"
    "fmt"
    "io/ioutil"
    "hash/crc32"
    //"sort"
)


type Log struct {
	dir      string
	log_file string
	fd       *os.File 
	buffer   []byte
	capacity int64 

}


func (l *Log) Append(record *Record) bool {
	bytes := record.ToBytes()
	n, _ := l.fd.Seek(0, 2)
	if n > l.capacity {
		return false
	}
	if n+ int64(len(bytes)) > l.capacity {
		l.NewLogFile()
	}
	l.fd.Write(bytes)
	l.fd.Sync()
	return true
}

func (l *Log) ReadAt(offset int64) {
	buf := [13]byte{}
	n, _ := l.fd.ReadAt(buf[:], offset)
	if n!=13 {
		panic("Supposed to read 13 bytes")
	}
	ksize, vsize := Byte2Int(buf[1:5]), Byte2Int(buf[5:9])
	op, checksum := string(buf[0:1]), Byte2Int(buf[9:13])
	offset += 13

	// 这边是要完成的。
	
	//cap or len	
}


func (l *log) ReadBatchRecordAt(offset int64, batchSize int) {
	
}

//当文件不够写的时候生成新的日志文件
func (l *Log) NewLogFile() bool {
	logFileName := l.log_file
	index := str2Int(findDigit(logFileName))
	logFileName = fmt.Sprintf("%d.log", index+1)
	fd, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("can't open log file %s", logFileName))
	}

	l.log_file = logFileName
	l.fd.Close()
	l.fd = fd
	return true
}

func NewLog(dir string, capacity int64) *Log {
	os.MkdirAll(dir, os.ModeDir)
	os.Chdir(dir)
	logName := genLogName(dir, capacity)
	fmt.Println(logName)
	fd, err := os.OpenFile( logName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("can't open log file %s", logName))
	
	}
	return &Log{dir:dir, log_file: logName, fd:fd, buffer: make([]byte, 1024), capacity:capacity}
}

// 验证一个日志的文件名称符合xxx.log的格式，xxx是一个整数
func isLogFileName(filename string) bool {
	i := 0
	for {
		if filename[i] >= '0' && filename[i] <= '9'{
			i+=1
		}else{
			break
		}

	}
	if i==0{
		return false
	}
	if filename[i:] != ".log" {
		return false
	}
	return true

}


func genLogName(dir string, capacity int64) string {

	files, _ :=ioutil.ReadDir(dir)
	v := []int{}
	for _, file := range files {
		if !file.IsDir() && isLogFileName(file.Name()) {
			v = append(v, str2Int(findDigit(file.Name())) )
		}
	}

	if len(v) == 0 {
		return "0.log"
	}

	index := maxInt(v)
	logName := fmt.Sprintf("%d.log", index)
	fd, err := os.OpenFile(logName,os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer fd.Close()
	if err != nil {
		panic(fmt.Sprintf("can't open file %s", logName))

	}
	finfo, _ := fd.Stat()
	if finfo.Size() < capacity {
		return logName 
	}
	logName = fmt.Sprintf("%d.log", index+1)
	return logName	

}


/*
func (l *Log) WriteAt(record *Record, offset int) {
	bytes = record.ToBytes()
	fd.WriteAt(bytes, offset)
	fd.Sync()
}

*/






type Record struct {
	key   string
	value string
	checksum string
	op    string
}

//Record的格式 op|ksize|vsize|checksum|key|value
func (r *Record) ToBytes() []byte {
	ksize := int2Byte(len(r.key))
	vsize := int2Byte(len(r.value))
	fmt.Println(ksize)
	//这里的转换可能会造成bug
	content := []byte(r.key+r.value)
	checksum := int2Byte(int(crc32.ChecksumIEEE(content)) )
	fmt.Println(checksum)

	return []byte( fmt.Sprintf("%s%s%s%s%s%s", 
						r.op, ksize, vsize, checksum, r.key, r.value))
		
}

func NewRecord(key string, value string, op string) *Record {
	return &Record{key:key, value:value, op:op}
}



func findDigit(filename string) string{
	ind := 0
	for _, c := range filename {
		if c >= '0' && c <= '9' {
			ind += 1
		}
	}
	return filename[0:ind]
}



func maxInt(v []int) int {
	m := v[0]
	for _, e := range v {
		if m < e {
			m = e
		}
	}
	return m
}

func minInt(v []int) int {
	m := v[0]
	for _, e := range v {
		if m > e {
			m = e
		}
	}
	return m
}



//256进制
func byte2Int(bytes []byte) int {
	s := 0
	for _, v := range bytes {
		s *= 256
		s += int(v)
	}
	return s
}


//生成4个字节
func int2Byte(i int) []byte {
    b := []byte{}
    if i==0 {
        b = append(b, byte(0))
        return b 
    }

    for i!=0 {
        mod := i%256
        b = append(b, byte(mod))
        i = i/256
    }
    for size := len(b); size < 4; size+=1{
    	b = append(b, byte(0))
    }
    reverse(b)
    return b
}

func reverse(bytes []byte) {
	size := len(bytes)
	for i :=0; i<= size/2; i+=1 {
		bytes[i], bytes[size-1-i] = bytes[size-1-i], bytes[i]
	}
}

//10进制
func str2Int(str string) int {
	s := 0
	for _, b := range str {
		s *= 10
		s += int(b) - int('0')
	}
	return s

}

func int2Str(i int) string {
	bytes := []byte{}
	if i== 0 {
		return "0"
	}
	for i!=0 {
		bytes = append(bytes, byte(i%10+ '0'))
		i = i/10
	}
	reverse(bytes)
	return string(bytes)

}