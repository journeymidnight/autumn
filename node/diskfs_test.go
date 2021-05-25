package node

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)
func TestPathName(t *testing.T) {
	/*
	p, err := ioutil.TempDir(os.TempDir(), "pathtest")
	require.Nil(t, err)
	defer os.RemoveAll(p)
	err = FormatDisk(p)
	require.Nil(t, err)

	err = ioutil.WriteFile(filepath.Join(p, "node_id"), []byte(fmt.Sprintf("%d", 100)), 0644)
	require.Nil(t, err)


	disk, err  := OpenDiskFS(p, 100)
	require.Nil(t, err)
	*/
	disk := &diskFS{
		baseDir: "/",
	}
	x := disk.pathName(28, "ext")
	fmt.Printf(x)
	require.Equal(t, "/68/28.ext", x)
}