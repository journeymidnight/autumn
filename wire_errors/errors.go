package wire_errors

import (
	"errors"

	"github.com/journeymidnight/autumn/proto/pb"
)

var (
	EndOfExtent = errors.New("EndOfExtent")
	EndOfStream = errors.New("EndOfStream")
	VersionLow = errors.New("version too low")
	NotLeader = errors.New("not a leader")
)


func FromPBCode(code pb.Code, des string) error {
	switch code {
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

