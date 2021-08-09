package main

import (
	"os"

	"github.com/journeymidnight/autumn/erasure_code"
	"github.com/journeymidnight/autumn/extent"
)



func main(){

	ex1, err := extent.OpenExtent(os.Args[1])
	if err != nil {
		panic(err)
	}
	ex2, err := extent.OpenExtent(os.Args[2])
	if err != nil {
		panic(err)
	}
	sourceExtents := make([]*extent.Extent, 3)
	sourceExtents[0]= ex1
	sourceExtents[2]= ex2
	ex3, err := extent.CreateExtent(os.Args[3], 13)
	if err != nil {
		panic(err)
	}
	ex3.Lock()
	err = erasure_code.ReedSolomon{}.RebuildECExtent(2, 1, sourceExtents, 1, ex3)
	if err != nil {
		panic(err.Error())
	}
	ex3.Unlock()

	/*
	enc, err := reedsolomon.New(2, 1)
	if err != nil {
		panic(err)
	}

	ex1, err := extent.OpenExtent(os.Args[1])
	if err != nil {
		panic(err)
	}
	ex2, err := extent.OpenExtent(os.Args[2])
	if err != nil {
		panic(err)
	}

	ex3, err := extent.CreateExtent(os.Args[3], 1000)
	ex3.Lock()
	if err != nil {
		panic(err)
	}

	shards := make([][]byte, 3)
	i := uint32(0)
	j := uint32(0)
	var blocks1 []*pb.Block
	var blocks2 []*pb.Block
	var done bool
	for !done {
		blocks1, _, i , err = ex1.ReadBlocks(i, 20, 32<<20)
		blocks2, _, j , err = ex2.ReadBlocks(j, 20, 32<<20)
		if i != j {
			panic(fmt.Sprintf("i != j, %d ,%d",i , j))
		}
		if err != nil {
			if err != wire_errors.EndOfExtent && err !=wire_errors.EndOfStream {
				panic(err)
			} else {
				done= true
			}
		}


		blocks3 := make([]*pb.Block, len(blocks1))
		for k := 0; k < len(blocks1); k++ {
			fmt.Printf("block len is %d\n", len(blocks1[k].Data))
			shards[1] = blocks1[k].Data
			shards[2] = blocks2[k].Data
			shards[0] = nil
			enc.Reconstruct(shards)
			blocks3[k] = &pb.Block{Data:shards[0]}
		}
		fmt.Println(i)
		var doSync bool
		if done {//last one
			doSync = true
		}
		_ ,  end , err := ex3.AppendBlocks(blocks3, doSync)
		fmt.Printf("end is %d, err is %v\n", end, err)
	}
	ex3.Unlock()
	ex3.Close()
	*/
	
}