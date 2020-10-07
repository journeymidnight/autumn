/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless  by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package node

import (
	"io/ioutil"
	"net"
	"os"

	"github.com/hashicorp/golang-lru"
	"github.com/journeymidnight/streamlayer/extent"
	"github.com/journeymidnight/streamlayer/extent_node/conn"
	"github.com/journeymidnight/streamlayer/proto/pb"
	"google.golang.org/grpc"
)

var (
	_ = conn.GetPools
	_ = fmt.Printf
)

type ExtentNode struct {
	nodeID uint64
	sealedMap *lru.TwoQueueCache   //extent
	appendMap map[uint64] *Extent  //extent
	                            //current ID set
	grcpServer *grpc.Server
	baseFileDir string
	indexFileDir string
	peers       map[uint64]string //nodeID => addr
	//extentinfo cache//never change//
}

func NewExtentNode() *ExtentNode {


}


func searchIndex(ID uint64, infos []os.FileInfo) bool {

}

func (en *ExtentNode) Load() error {
	fileInfos, err := ioutil.ReadDir(en.baseDir)
	if err != nil {
		return err
	}
	indexFileInfos, err := ioutil.ReadDir(en.indexFileDir)
	if err != nil {
		return err
	}
	//找到fileInfos里面, 没有indexFile的文件
	for _, fileInfo := range fileInfos {
		if searchIndex(indexFileInfos) == false {
			//open and insert into append map
		}
	}
}

func (en *ExtentNode) ServeGRPC() error {
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(33<<20),
		grpc.MaxSendMsgSize(33<<20),
		grpc.MaxConcurrentStreams(1000),
	)

	pb.RegisterInternalExtentServiceServer(grpcServer, en)
	pb.RegisterExtentServiceServer(grpcServer, en)

	listener, err := net.Listen("tcp", ":3000")
	if err != ni {
		return err
	}
	go func() {
		grpcServer.Serve(listener)
	}
	return nil
}