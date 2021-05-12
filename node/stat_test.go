package node

import (
	"fmt"
	"testing"

	"github.com/dustin/go-humanize"
)

func TestStat(t *testing.T) {
	total, free, err := getDiskInfo(".")
	fmt.Printf("disk usage on current disk: %s, %s, %v", humanize.Bytes(total), humanize.Bytes(free), err)
}