package stream_manager

import (
	"context"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
)

//go routine
func (sm *StreamManager) routineUpdateDF() {

	ticker := utils.NewRandomTicker(time.Minute, 2 * time.Minute)

	var ctx context.Context
	var cancel context.CancelFunc
	defer func() {
		xlog.Logger.Infof("routineUpdateDF quit")
	}()

	xlog.Logger.Infof("routineUpdateDF started")
	for {
		select {
			case <- sm.stopper.ShouldStop():
				if cancel != nil{
					cancel()
				}
				return
			case <- ticker.C:
				ctx, cancel = context.WithCancel(context.Background())
				nodes := sm.getNodeStatus(true)
				stopper := utils.NewStopper()
				for i := range nodes {
					node := nodes[i]
					stopper.RunWorker(func(){
						conn := node.GetConn()
						pctx, pCancel := context.WithTimeout(ctx, 5 * time.Second)
						client := pb.NewExtentServiceClient(conn)
						res, err := client.Df(pctx, &pb.DfRequest{})
						pCancel()
						if err != nil {
							xlog.Logger.Infof("remote server %s not response or to", node.Address)
							return
						}
						if res.Code != pb.Code_OK {
							xlog.Logger.Infof("remote server has error %s", res.CodeDes)
							return
						}
						node.SetFree(res.Df.Free)
						node.SetTotal(res.Df.Total)
					})

				}
				stopper.Wait()


		}
	}
}


func (sm *StreamManager) routineFixReplics() {
	//loop over all extent, if extent has dead node.
	//insert task into "task pool"
	/*
	ticker := utils.NewRandomTicker(time.Minute, 2 * time.Minute)

	var ctx context.Context
	var cancel context.CancelFunc
	defer func() {
		xlog.Logger.Infof("routineFixReplics quit")
	}()

	xlog.Logger.Infof("routineFixReplics started")

	for {
		select {
			case <- sm.stopper.ShouldStop():
				cancel()
				return
			case <- ticker.C:
				ctx, cancel = context.WithCancel(context.Background())
				sm.clone

		}
	}
	*/
}


func (sm *StreamManager) routineDispatchTask() {
	//loop over task pool, assign a task to a living node
}


