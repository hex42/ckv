package kv

import (
    "os"
    "fmt"
    "io/ioutil"
    "hash/crc32"
    "math/rand"
)


type Log struct {
	dir      string
	logFile  string 
	cacheFd  map[string]*os.File 
	fd       *os.File 
	buffer   []byte
	capacity int64	//单个log的最大容量 
	sync     bool   //Write操作是否是同步的
	syncSize int64  //允许缓存的大小
	writeSize int64 //已经缓存的大小
	writeAmount int64 //对于当前的logFile已经Append的数量，包括已经刷到磁盘上的和在内存缓存的

}

type offSet struct {
	logFile  string
	off      int64
}
 
//如果当个kv的大小超过capacity的时候 offset的off字段设置成-1，后面可以改造成支持kv分片的格式
func (l *Log) Append(record *Record) *offSet {
	bytes := record.ToBytes()
	size := int64(len(bytes))
	
	if size > l.capacity {
		return &offSet{logFile: l.logFile, off: -1}
	}

	if l.writeAmount + size > l.capacity {
		l.NewLogFile()
	}
	off := &offSet{logFile: l.logFile, off: l.writeAmount}
	n, err := l.fd.WriteAt(bytes, l.writeAmount)
	if err != nil || int64(n) != size {
		panic("Got error on write")
	} 
	l.writeAmount += size
	l.writeSize += size

	if l.sync {
		l.fd.Sync()
	
	}else{
		if l.writeSize >  l.syncSize {
			l.fd.Sync()
			l.writeSize = 0
		}
	}
	return off
}

// 当读到eof时,返回nil
func (l *Log) ReadAt(offset *offSet) *Record {
	off, logFile := offset.off, offset.logFile

	fd, ok := l.cacheFd[logFile]
	if !ok {
		if logFile == l.logFile {
			fd = l.fd
		}else{
			fd, _ = os.Open(logFile)
			l.cacheFd[logFile] = fd
		}
	}

	fileInfo, _ := fd.Stat()
	if fileInfo.Size() == off {
		return nil 
	}

	if off > fileInfo.Size() {
		errMsg := fmt.Sprintf("offset %s:%d filesize %d", offset.logFile, offset.off, fileInfo.Size())
		panic(errMsg)
	}

	//fmt.Printf("offset %s:%d\n", offset.logFile, offset.off)

	buf := l.buffer[0:13]
	n, _ := fd.ReadAt(buf, off)
	if n != 13 {
		panic(fmt.Sprintf("broken log file %s", logFile))
	}

	ksize, vsize := byte2Int(buf[1:5]), byte2Int(buf[5:9])
	op, checksum := string(buf[0:1]), string(buf[9:13])

	//fmt.Printf("op:ksize:vsize %s %d %d\n", op, ksize, vsize)

	off += 13
	buf = l.buffer[0:ksize]
	n, _ = fd.ReadAt(buf, off)
	if n != ksize {
		panic(fmt.Sprintf("broken log file %s", logFile))
	}

	key := string(buf)
	value := ""
	off += int64(ksize)
	if vsize != 0 {
		buf = l.buffer[0:vsize]
		n, _ = fd.ReadAt(buf, off)
		if n != vsize {
			panic(fmt.Sprintf("broken log file %s", logFile))
		}
		value = string(buf)
	}

	off += int64(vsize) 
	//fmt.Println("each record:", op,len(checksum), ksize, vsize,key, value, off)
	return &Record{op: op, key: key, value: value, checksum: checksum}
}


func (l *Log) ReadLogFile(logFile string) ([]*Record, []*offSet) {
	offset:= &offSet{logFile: logFile, off: 0}
	records := []*Record{}
	offSets := []*offSet{}

	for {
		record := l.ReadAt(offset)
		if record == nil {
			break
		}
		offSets = append(offSets, &offSet{logFile: offset.logFile, off: offset.off})
		records = append(records, record)
		offset.off += record.Size()
		//fmt.Println(offset.off)
	}

	return records, offSets
}


func (l *Log) Close() bool {
	for _, fd := range l.cacheFd {
		fd.Close()
	}
	l.fd.Close()
	return true
}


func (l *Log) Size() int64 {
	size := int64(0)
	logFiles := l.AllLogFiles()
	for _, logFile := range logFiles {
		if logFile != l.logFile {
			fd, ok := l.cacheFd[logFile]
			if !ok {
				fd, _ = os.Open(logFile)
				l.cacheFd[logFile] = fd
			}
			fileInfo, _ := fd.Stat()
			size += fileInfo.Size()
			continue
		}
		fileInfo, _ := l.fd.Stat()
		size += fileInfo.Size()
	}
	return size

}

//删除日志文件，同时删除缓存的fd
func (l *Log) RemoveLogFile(logFile string) bool {
	if logFile == l.logFile {
		return false
	}

	logFiles := l.AllLogFiles()
	b := false
	for i:=0; i<len(logFiles)-1; i+=1 {
		if logFiles[i] == logFile {
			b=true
		}
	}

	if !b {
		return false
	}

	fd, ok := l.cacheFd[logFile]
	if ok {
		fd.Close()
		delete(l.cacheFd, logFile)
	}
	os.Remove(logFile)
	return true 

}



//当文件不够写的时候生成新的日志文件
func (l *Log) NewLogFile() bool {
	oldLogFileName := l.logFile
	index := str2Int(findDigit(oldLogFileName))
	logFileName := fmt.Sprintf("%d.log", index+1)
	fd, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("can't open log file %s", logFileName))
	}

	l.logFile = logFileName
	l.writeSize = 0
	l.writeAmount = 0
	l.fd.Sync()
	l.fd.Close()
	l.fd = fd
	fd, err = os.Open(oldLogFileName)
	if err != nil {
		panic(fmt.Sprintf("can't open log file %s", oldLogFileName))
	}

	l.cacheFd[oldLogFileName] = fd
	return true
}



//得到所有的日志文件名称
func (l *Log) AllLogFiles() []string {
	files, _ := ioutil.ReadDir(l.dir)
	logFiles := []string{}
	for _, file := range files {
		if !file.IsDir() && isLogFileName(file.Name()) {
			logFiles = append(logFiles, file.Name())
		}
	}
	v := make([]int, len(logFiles))
	for i, _ := range v {
		v[i] = str2Int(findDigit(logFiles[i]))
	}
	
	bubbleSort(v)

	for i, _ := range v {
		logFiles[i] = fmt.Sprintf("%d.log", v[i])
	}
	return logFiles
}



// 验证一个日志的文件名称符合x.log的格式，x是一个整数
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

// 如果sync = true， syncSize 应该被设置成 0
func NewLog(dir string, capacity int64, sync bool, syncSize int64) *Log {
	os.MkdirAll(dir, os.ModeDir)
	os.Chdir(dir)
	logName := genLogName(dir, capacity)
	fmt.Println("logName", logName)
	fd, err := os.OpenFile(logName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("can't open log file %s", logName))
	
	}
	fileInfo, _ := fd.Stat()
	writeAmount := fileInfo.Size()

	fmt.Println("writeAmount", writeAmount)
	return &Log{dir:dir, logFile:logName, cacheFd:map[string]*os.File{}, fd:fd, 
			buffer: make([]byte, 1024), capacity:capacity, sync:sync, 
			syncSize:syncSize, writeSize:0, writeAmount:writeAmount }
}

// 生成最新的日志名称
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



type Record struct {
	op    	 string
	key   	 string
	value 	 string
	checksum string
}


//Record的格式 op|ksize|vsize|checksum|key|value op is limited to P or D
func (r *Record) ToBytes() []byte {
	ksize := int2Byte(len(r.key))
	vsize := int2Byte(len(r.value))	

	return []byte( fmt.Sprintf("%s%s%s%s%s%s", 
						r.op, ksize, vsize, r.checksum, r.key, r.value))
		
}

//8 是4个字节的ksize+4个字节的vsize
func (r *Record) Size() int64 {
	return int64(len(r.op) + len(r.checksum) + len(r.key) + len(r.value) + 8)
}



func NewRecord(op string, key string, value string) *Record {
	if op == "D" {
		value = ""
	}
	content := []byte(key+value)
	checksum := int2Byte(int(crc32.ChecksumIEEE(content)) )
	return &Record{key:key, value:value, op:op, checksum: string(checksum) }
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


func bubbleSort(v []int) {
	size := len(v)
	for i:=0; i< size; i+=1 {
		for j:=size-1; j>i; j-=1 {
			if v[j] < v[j-1]{
				v[j], v[j-1] = v[j-1], v[j]
			}
		}
	}

}


func quickSort(v []int) {
	size := len(v)
	//fmt.Println(size)
	if size < 15 {
		bubbleSort(v) 
		return
	}

	pivot := rand.Int() % size 
	e := v[pivot]
	begin, end := 0, size - 1
	for begin < end {
		for begin < end {
			if v[begin] > e {
				break
			}

			begin += 1
		}

		for begin < end {
			if v[end] <=  e {
				break
			}
			end -= 1
		}
		v[begin], v[end] = v[end], v[begin]
	}

	if v[begin] <= e {
		begin += 1	
	}

	quickSort(v[0:begin])
	if begin < size {
		quickSort(v[begin:size])
	}

	return 

}

