package y

import (
	"bytes"
	"fmt"
	"testing"
)

func TestDecodeVS(t *testing.T) {
	vs := ValueStruct{
		Meta: 1,
		ExpiresAt: 0,
		Value: []byte("a"),
	}
	buf := new(bytes.Buffer)
	vs.Write(buf)

	fmt.Printf("a: %d\n", buf.Len())
	fmt.Printf("b: %d\n", vs.EncodedSize())

	var vsx ValueStruct
	vsx.Decode(buf.Bytes())
	fmt.Printf("%+v\n", vs)
}
