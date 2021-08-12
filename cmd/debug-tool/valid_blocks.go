package main

import (
	"fmt"
	"math/rand"
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

	if validEnd != ex.CommitLength() {
		fmt.Printf("this file from %d ~ %d is valid", offset, validEnd)
		return 
	}
	
	//call ex.ReadBlocks until meet error
	var blocks []*pb.Block
	var end uint32
	err = nil
	n := 0
	var ends []uint32
	for err == nil {
		blocks, _, end, err = ex.ReadBlocks(uint32(offset), 10, 120<<20)
		//fmt.Printf("possible end %d\n", end)
		ends = append(ends, end)
		offset = int(end)
		n += len(blocks)
	}
	//chose random one from ends
	end = ends[rand.Intn(len(ends))]
	fmt.Printf("possible in middle end is %d\n", end)
	fmt.Printf("end of blocks %d, total number of blocks is %d\n", offset, n)

}
