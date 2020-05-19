package client

import (
	"fmt"
	uuid "github.com/blackbeans/go-uuid"
	"github.com/golang/snappy"
)

func Decompress(body []byte) ([]byte, error) {

	l, err := snappy.DecodedLen(body)
	if nil != err {
		return nil, err
	}
	if l%256 != 0 {
		l = (l/256 + 1) * 256
	}
	dest := make([]byte, l)
	decompressData, err := snappy.Decode(dest, body)
	if nil != err {
		return nil, err
	}
	return decompressData, nil
}

//snapp压缩
func Compress(body []byte) ([]byte, error) {

	l := snappy.MaxEncodedLen(len(body))
	if l%256 != 0 {
		l = (l/256 + 1) * 256
	}

	dest := make([]byte, l)
	return snappy.Encode(dest, body), nil
}

//生成messageId uuid
func MessageId() string {
	id := uuid.NewRandom()
	if id == nil || len(id) != 16 {
		return ""
	}
	b := []byte(id)
	return fmt.Sprintf("%08x%04x%04x%04x%012x",
		b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
