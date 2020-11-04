package rangepartition

import "github.com/journeymidnight/autumn/streamclient"

type RangePartition struct {
	metadataStream *streamclient.StreamClient
	rawDataStream  *streamclient.StreamClient
	blobDataStream *streamclient.StreamClient
}

/*
func (rp *RangePartition) WriteRPC() {
	ch := make(chan XXX)
	append(Entry, func() {
		//blalbal
		ch <- ok
	})
	<-ch
	return
}
*/
