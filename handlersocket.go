package handlersocket

import (
  "bytes"
  "net"
  "strconv"
  "bufio"
  "fmt"
  "strings"
)

var (
  LINEFEED = byte(0x0a)
  LINEFEEDstr = "\x0a"
  FIELDSEP = []byte("\x09")
  NULL = []byte("\x00")

  Debug = false
)

func verbose(s string, arg ...interface{}) {
  if Debug {
    fmt.Printf(s, arg...)
  }
}

func encode(str []byte) []byte {
  buf := new(bytes.Buffer)
  for _, c := range str {
    if c > byte(0x0f) {
      buf.WriteByte(c)
    } else {
      buf.WriteByte(byte(0x01))
      buf.WriteByte(c | byte(0x43))
    }
  }
  return buf.Bytes()
}

func split(line []byte) [][]byte {
  line = bytes.TrimRight(line, LINEFEEDstr)
  return bytes.Split(line, FIELDSEP)
}

func New(addr string, rdPort int, rwPort int) (*HandlerSocket, error) {
  self := &HandlerSocket{
    indexIdChan: make(chan *indexIdQuery),
    indexIdEnd: make(chan bool),
  }
  var err error
  self.rdConn, err = net.Dial("tcp", addr + ":" + strconv.Itoa(rdPort))
  if err != nil {
    return nil, err
  }
  self.rwConn, err = net.Dial("tcp", addr + ":" + strconv.Itoa(rwPort))
  if err != nil {
    return nil, err
  }
  self.rdReader = bufio.NewReader(self.rdConn)
  self.rwReader = bufio.NewReader(self.rwConn)
  self.startIndexProvider()
  return self, nil
}

type HandlerSocket struct {
  rdConn net.Conn
  rwConn net.Conn
  rdReader *bufio.Reader
  rwReader *bufio.Reader
  indexIdChan chan *indexIdQuery
  indexIdEnd chan bool
}

type indexIdQuery struct {
  signature string
  ret chan int
}

const (
  tRd = iota
  tRw
)

func packFields(fields ...interface{}) []byte {
  encodedFields := make([][]byte, 0)
  for _, v := range fields {
    switch v.(type) {
    case nil:
      encodedFields = append(encodedFields, NULL)
    case string:
      encodedFields = append(encodedFields, encode([]byte(v.(string))))
    case []byte:
      encodedFields = append(encodedFields, encode(v.([]byte)))
    case int:
      encodedFields = append(encodedFields, encode([]byte(strconv.Itoa(v.(int)))))
    }
  }
  line := bytes.Join(encodedFields, FIELDSEP)
  line = append(line, LINEFEED)
  return line
}

func (self *HandlerSocket) Request(reqType int, fields ...interface{}) ([][]byte, error) {
  line := packFields(fields...)
  verbose("->->->-> %s | %#v\n", line, line)
  var response []byte
  var err error
  if reqType == tRd {
    self.rdConn.Write(line)
    response, err = self.rdReader.ReadBytes(LINEFEED)
  } else {
    self.rwConn.Write(line)
    response, err = self.rwReader.ReadBytes(LINEFEED)
  }
  if err != nil {
    return nil, err
  }
  verbose("<=<=<=<= %s | %#v\n", response, response)
  return split(response), nil
}

func (self *HandlerSocket) Rd(fields ...interface{}) ([][]byte, error) {
  return self.Request(tRd, fields...)
}

func (self *HandlerSocket) Rw(fields ...interface{}) ([][]byte, error) {
  return self.Request(tRw, fields...)
}

func (self *HandlerSocket) Close() {
  self.rdConn.Close()
  self.rwConn.Close()
  self.indexIdEnd <- true
}

func (self *HandlerSocket) OpenIndex(dbname string, tablename string, indexname string, columns ...string) int {
  retChan := make(chan int, 1)
  self.indexIdChan <- &indexIdQuery{genSignature(dbname, tablename, indexname, columns...), retChan}
  indexId := <-retChan
  response, err := self.Rd("P", strconv.Itoa(indexId), dbname, tablename, indexname, strings.Join(columns, ","))
  if err != nil {
    return -1
  }
  if bytes.Compare(response[0], []byte("0")) != 0 || bytes.Compare(response[1], []byte("1")) != 0 {
    return -1
  }
  response, err = self.Rw("P", strconv.Itoa(indexId), dbname, tablename, indexname, strings.Join(columns, ","))
  if err != nil {
    return -1
  }
  if bytes.Compare(response[0], []byte("0")) != 0 || bytes.Compare(response[1], []byte("1")) != 0 {
    return -1
  }
  return indexId
}

func (self *HandlerSocket) startIndexProvider() {
  go func() {
    index := 1
    mapping := make(map[string]int)
    for {
      select {
      case req := <-self.indexIdChan:
        i, exists := mapping[req.signature]
        if exists {
          req.ret <- i
        } else {
          mapping[req.signature] = index
          req.ret <- index
          index++
        }
      case <-self.indexIdEnd:
        break
      }
    }
  }()
}

func genSignature(dbname string, tablename string, indexname string, columns ...string) (sig string) {
  sig += dbname + tablename + indexname
  for _, column := range columns {
    sig += column
  }
  return sig
}
