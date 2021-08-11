package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/journeymidnight/autumn/extent"
	"github.com/journeymidnight/autumn/proto/pb"
)

func main() {
	//open extent and valid all blocks
	//extent := openExtent(extentPath)
	//validBlocks(extent)
	extentPath := os.Args[1]
	offset, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
		return
	}
	ex, err := extent.OpenExtent(extentPath)
	if err != nil {
		panic(err)
	}

	fmt.Printf("commit length is %d\n", ex.CommitLength())

	validEnd, err := ex.ValidAllBlocks(int64(offset))
	fmt.Println(err)
	fmt.Printf("end of valid blocks %d\n", validEnd)

	//call ex.ReadBlocks until meet error
	var blocks []*pb.Block
	var end uint32
	err = nil
	n := 0
	for err == nil {
		blocks, _, end, err = ex.ReadBlocks(uint32(offset), 100, 120<<20)
		offset = int(end)
		n += len(blocks)
	}
	fmt.Printf("end of blocks %d, total number of blocks is %d\n", offset, n)

}
