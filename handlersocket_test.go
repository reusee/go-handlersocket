package handlersocket

import (
  "testing"
  "fmt"
  "bytes"
  "time"
  "strconv"
)

const (
  rdPort = 45678
  rwPort = 56789
)

func equal(left [][]byte, right []string) bool {
  if len(left) != len(right) {
    return false
  }
  for i, v := range left {
    if bytes.Compare(v, []byte(right[i])) != 0 {
      return false
    }
  }
  return true
}

func TestEncode(t *testing.T) {
  fmt.Printf("fmt used\n")
  Debug = true
  str := []byte("\x03")
  encoded := encode(str)
  if bytes.Compare(encoded, []byte("\x01\x43")) != 0 {
    t.Fail()
  }
}

func TestSplit(t *testing.T) {
  line := []byte("\x09\x09")
  splited := split(line)
  if len(splited) != 3 {
    t.Fail()
  }
  for _, v := range splited {
    if bytes.Compare(v, []byte("")) != 0 {
      t.Fail()
    }
  }
}

func TestSplit2(t *testing.T) {
  line := []byte("\x09\x0a")
  splited := split(line)
  if len(splited) != 2 {
    t.Fail()
  }
  for _, v := range splited {
    if bytes.Compare(v, []byte("")) != 0 {
      t.Fail()
    }
  }
}

func TestNew(t *testing.T) {
  New("localhost", rdPort, rwPort)
}

func TestPackFields(t *testing.T) {
  line := packFields([]byte(""))
  if bytes.Compare(line, []byte("\x0a")) != 0 {
    t.Fail()
  }
  line = packFields([]byte(""), nil, []byte(""))
  if bytes.Compare(line, []byte("\x09\x00\x09\x0a")) != 0 {
    t.Fail()
  }
}

func TestOpenIndex(t *testing.T) {
  hs := New("localhost", rdPort, rwPort)
  response := hs.Request(tRd, []byte("P"), []byte("1"), []byte("test"), []byte("kvs"), []byte("PRIMARY"),
    []byte("content"), []byte("id"))
  if !equal(response, []string{"0", "1"}) {
    t.Fail()
  }
}

func TestInsert(t *testing.T) {
  hs := New("localhost", rdPort, rwPort)
  hs.Request(tRw, []byte("P"), []byte("1"), []byte("test"), []byte("kvs"), []byte("PRIMARY"), []byte("id,content"))
  for i := 0; i < 20; i++ {
    id := strconv.FormatInt(time.Now().UnixNano(), 10)
    resp := hs.Request(tRw, []byte("1"), []byte("+"), []byte("2"), []byte(id), []byte("CONTENT"))
    if !equal(resp, []string{"0", "1"}) {
      t.Fail()
    }
  }
}

func TestGetting(t *testing.T) {
  hs := New("localhost", rdPort, rwPort)
  hs.Request(tRd, []byte("P"), []byte("1"), []byte("test"), []byte("kvs"), []byte("PRIMARY"), []byte("id,content"))
  hs.Request(tRd, []byte("1"), []byte("="), []byte("1"), []byte("ID"))
  hs.Request(tRd, []byte("1"), []byte("="), []byte("1"), []byte("你好啊"))
}
