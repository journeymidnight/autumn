package rangepartition

import "github.com/journeymidnight/autumn/streamclient"

type RangePartition struct {
	metadataStream *streamclient.StreamClient
	rawDataStream  *streamclient.StreamClient
	blobDataStream *streamclient.StreamClient
}

func (rp *RangePartition) Write() {
	//log.Append
	//wait
}
