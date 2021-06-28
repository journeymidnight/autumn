package wire_errors

import (
	"errors"

	"github.com/journeymidnight/autumn/proto/pb"
)

var (
	EndOfExtent = errors.New("EndOfExtent")
	EndOfStream = errors.New("EndOfStream")
	VersionLow = errors.New("extent version too low")
	NotLeader = errors.New("not a leader")
	LockedByOther = errors.New("LockedByOther")
	StreamVersionLow = errors.New("stream version low")
)


func FromPBCode(code pb.Code, des string) error {
	switch code {
	case pb.Code_StreamVersionLow:
		return StreamVersionLow
	case pb.Code_LockedByOther:
		return LockedByOther
	case pb.Code_EndOfExtent:
		return EndOfExtent
	case pb.Code_EndOfStream:
		return EndOfExtent
	case pb.Code_EVersionLow:
		return VersionLow
	case pb.Code_NotLEADER:
		return NotLeader
	case pb.Code_OK:
		return nil
	default:
		return errors.New(des)
	}

}
func ConvertToPBCode(err error) (pb.Code, string) {
	switch err {
	case StreamVersionLow:
		return pb.Code_StreamVersionLow, err.Error()
	case LockedByOther:
		return pb.Code_LockedByOther, err.Error()
	case EndOfExtent:
		return pb.Code_EndOfExtent, err.Error()
	case EndOfStream:
		return pb.Code_EndOfStream, err.Error()
	case VersionLow:
		return pb.Code_EVersionLow, err.Error()
	case NotLeader:
		return pb.Code_NotLEADER, err.Error()
	case nil:
		return pb.Code_OK, ""
	default:
		return pb.Code_ERROR, err.Error()
	}
}

