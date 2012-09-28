package handlersocket

import (
  "testing"
  "fmt"
  "bytes"
)

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
  New("localhost", 9998, 9999)
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
  hs := New("localhost", 9998, 9999)
  response := hs.Request(tRd, []byte("P"), []byte("1"), []byte("test"), []byte("kvs"), []byte("PRIMARY"),
    []byte("content"), []byte("id"))
  if bytes.Compare(response, []byte("\x30\x09\x31\x0a")) != 0 {
    t.Fail()
  }
}
