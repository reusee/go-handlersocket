package handlersocket

import (
  "bytes"
  "net"
  "strconv"
  "log"
  "bufio"
  "fmt"
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

func New(addr string, rdPort int, rwPort int) *HandlerSocket {
  hs := new(HandlerSocket)
  var err error
  hs.rdConn, err = net.Dial("tcp", addr + ":" + strconv.Itoa(rdPort))
  if err != nil {
    log.Fatal("connect error")
  }
  hs.rwConn, err = net.Dial("tcp", addr + ":" + strconv.Itoa(rwPort))
  if err != nil {
    log.Fatal("connect error")
  }
  hs.rdReader = bufio.NewReader(hs.rdConn)
  hs.rwReader = bufio.NewReader(hs.rwConn)
  return hs
}

type HandlerSocket struct {
  rdConn net.Conn
  rwConn net.Conn
  rdReader *bufio.Reader
  rwReader *bufio.Reader
}

const (
  tRd = iota
  tRw
)

func packFields(fields ...[]byte) []byte {
  encodedFields := make([][]byte, 0)
  for _, v := range fields {
    if v == nil {
      encodedFields = append(encodedFields, NULL)
    } else {
      encodedFields = append(encodedFields, encode(v))
    }
  }
  line := bytes.Join(encodedFields, FIELDSEP)
  line = append(line, LINEFEED)
  return line
}

func (hs *HandlerSocket) Request(reqType int, fields ...[]byte) []byte {
  line := packFields(fields...)
  verbose("== Request == %s | %#v\n", line, line)
  var response []byte
  var err error
  if reqType == tRd {
    hs.rdConn.Write(line)
    response, err = hs.rdReader.ReadBytes(LINEFEED)
  } else {
    hs.rwConn.Write(line)
    response, err = hs.rwReader.ReadBytes(LINEFEED)
  }
  if err != nil {
    log.Fatal("query error")
  }
  return response
}
