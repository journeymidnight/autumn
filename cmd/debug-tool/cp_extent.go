package main

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/utils"
)

//cp_extent <src> <dest> <offset>
//offset一定是一个合法的block的offset(包括header)
func main() {
	//cp file from offset of argv[1] to argv[2]
	src := os.Args[1]
	dest := os.Args[2]
	//parse offset to int64 from argv[3]
	offset, err :=  strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}
	ex, err := extent.CreateExtent(dest, 0)
	if err != nil {
		panic(err)
	}

	ex.Lock()
	w := ex.GetRawWriter()
	
	srcFile , err := os.Open(src)
	if err != nil {
		panic(err)
	}
	defer srcFile.Close()

	x := utils.Floor(uint32(offset), 64<<10)
	
	padding := uint32(offset) - x
	
	fmt.Printf("start padding is %d\n", padding)

	srcFile.Seek(int64(x), 0)
	n, err := io.Copy(w, srcFile)
	fmt.Printf("copied %d bytes\n", n)
	ex.Unlock()
	if err != nil {
		panic(err)
	}
	ex.Close()
	
}
