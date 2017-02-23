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
	logFile  string //xxx.log
	cacheFd  map[string]*os.File
	fd       *os.File 
	buffer   []byte
	capacity int64	//单个log的最大容量 
	sync     bool   //Write操作是否市同步的
	syncSize int64  //允许缓存的大小
	writeSize int64 //已经缓存的大小

}

type offSet struct {
	logFile  string
	off      int64
}


func (l *Log) Append(record *Record) bool {
	bytes := record.ToBytes()
	n, _ := l.fd.Seek(0, 2)
	if n > l.capacity {
		panic(fmt.Sprintf("kv item too big %d bytes", len(bytes)))
	}
	if n+ int64(len(bytes)) > l.capacity {
		l.NewLogFile()
	}

	if l.sync {
		l.fd.Write(bytes)
		l.fd.Sync()
	
	}else{
		l.fd.Write(bytes)
		l.writeSize += len(bytes)
		if l.writeSize >  l.syncSize {
			l.fd.Sync()
			l.writeSize = 0
		}
	}
	return true
}


func (l *Log) ReadAt(offset *offSet) *Record, *offSet {

	logFile := offset.logFile
	fd, ok := l.cacheFd[logFile]
	if !ok {
		fd = os.Open(logFile)
		l.cacheFd[logFile] = fd
	}

	buf := l.buffer[0:13]
	n, _ := l.fd.ReadAt(buf, offset)
	if n!=13 {
		panic(fmt.Sprintf("broken log file %s", logFile))
	}
	ksize, vsize := Byte2Int(buf[1:5]), Byte2Int(buf[5:9])
	op, checksum := string(buf[0:1]), string(buf[9:13])
	offset += 13
	buf = l.buffer[0:ksize]
	n, _ = l.fd.ReadAt(buf, offset)
	if n != ksize {
		panic(fmt.Sprintf("broken log file %s", logFile))
	}
	key := string(buf)
	value := ""

	if vsize != 0 {
		offset += ksize
		buf = l.buffer[0:vsize]
		n, _ = l.fd.ReadAt(buf, offset)
		if n != vsize {
			panic(fmt.Sprintf("broken log file %s", logFile))
		}
		value = string(buf)
	}

	offset += vsize

	//check the checksum

	return &Record{key: key, value: value, checksum: checksum, op: op},
			&offSet{logFile: logFile, off: offset}

}



//read all records in a log file 
func (l *Log) ReadRecords(logFile string) {


}

//当文件不够写的时候生成新的日志文件
func (l *Log) NewLogFile() bool {
	oldLogFileName := l.logFile
	index := str2Int(findDigit(oldLogFileName))
	logFileName = fmt.Sprintf("%d.log", index+1)
	fd, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("can't open log file %s", logFileName))
	}

	l.logFile = logFileName
	l.fd.Close()
	l.fd = fd
	fd, err := os.Open(oldLogFileName)
	if err != nil {
		panic(fmt.Sprintf("can't open log file %s", oldLogFileName)
	}
	
	l.cacheFd[oldLogFileName] = fd
	return true
}

func NewLog(dir string, capacity int64, sync bool, syncSize int64) *Log {
	os.MkdirAll(dir, os.ModeDir)
	os.Chdir(dir)
	logName := genLogName(dir, capacity)
	fmt.Println(logName)
	fd, err := os.OpenFile( logName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("can't open log file %s", logName))
	
	}
	return &Log{dir:dir, logFile: logName, fd:fd, buffer: make([]byte, 1024), 
				capacity:capacity, sync: sync, syncSize: syncSize}
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

// 得到最新的日志名称
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



//Record的格式 op|ksize|vsize|checksum|key|value op id limited to P or D
func (r *Record) ToBytes() []byte {
	ksize := int2Byte(len(r.key))
	vsize := int2Byte(len(r.value))	

	return []byte( fmt.Sprintf("%s%s%s%s%s%s", 
						r.op, ksize, vsize, checksum, r.key, r.value))
		
}

func (r *Record) Size() int {
	return len(r.op) + len(r.checksum) + len(r.key) + len(r.value)
}



func NewRecord(key string, value string, op string) *Record {
	//这里的转换可能会造成bug
	content := []byte(r.key+r.value)
	checksum := int2Byte(int(crc32.ChecksumIEEE(content)) )
	return &Record{key:key, value:value, op:op, checksum: }
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
