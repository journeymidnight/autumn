package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/journeymidnight/autumn/etcd_utils"
	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/proto/pspb"
	"github.com/pkg/errors"
)

type KV struct {
	Key   string
	Value string
}

/*
ETCD example list:
AutumnPMIDKey
AutumnPMLeader/34e78663bbfa104
AutumnSMIDKey
AutumnSMLeader/34e78663bbfa106
PART/2/logStream
PART/2/parent
PART/2/range
PART/2/rowStream
PART/2/tables
PSSERVER/1
extents/5
extents/7
nodes/1
nodes/2
nodes/3
streams/4
streams/6
*/

//receiveData from ETCD
func receiveData(client *clientv3.Client) []KV {
	var data []KV
	kvs, err := etcd_utils.EtcdRange(client, "")
	if err != nil {
		panic(err)
	}
	for _, kv := range kvs {
		if strings.HasPrefix(string(kv.Key), "PART") {
			data = append(data, parseParts(kv))
		} else if strings.HasPrefix(string(kv.Key), "AutumnSMIDKey") || strings.HasPrefix(string(kv.Key), "AutumnPMIDKey") {
			d := KV{
				Key:   string(kv.Key),
				Value: fmt.Sprintf("%d", binary.BigEndian.Uint64(kv.Value)),
			}
			data = append(data, d)
		} else if strings.HasPrefix(string(kv.Key), "AutumnSMLeader") || strings.HasPrefix(string(kv.Key), "AutumnPMLeader") {
			var x pb.MemberValue
			if err := x.Unmarshal(kv.Value); err != nil {
				panic(err)
			}
			d := KV{
				Key:   string(kv.Key),
				Value: fmt.Sprintf("%+v", x),
			}
			data = append(data, d)
		} else if strings.HasPrefix(string(kv.Key), "PSSERVER") {
			var x pspb.PSDetail
			if err := x.Unmarshal(kv.Value); err != nil {
				panic(err)
			}
			d := KV{
				Key:   string(kv.Key),
				Value: fmt.Sprintf("%+v", x),
			}
			data = append(data, d)
		} else if strings.HasPrefix(string(kv.Key), "extents") {
			var x pb.ExtentInfo
			if err := x.Unmarshal(kv.Value); err != nil {
				panic(err)
			}
			d := KV{
				Key:   string(kv.Key),
				Value: fmt.Sprintf("%+v", x),
			}
			data = append(data, d)
		} else if strings.HasPrefix(string(kv.Key), "nodes") {
			var x pb.NodeInfo
			if err := x.Unmarshal(kv.Value); err != nil {
				panic(err)
			}
			d := KV{
				Key:   string(kv.Key),
				Value: fmt.Sprintf("%+v", x),
			}
			data = append(data, d)

		} else if strings.HasPrefix(string(kv.Key), "streams") {
			var x pb.StreamInfo
			if err := x.Unmarshal(kv.Value); err != nil {
				panic(err)
			}
			d := KV{
				Key:   string(kv.Key),
				Value: fmt.Sprintf("%+v", x),
			}
			data = append(data, d)
		} else {
			continue
			//panic("unkown key...")
		}
	}
	return data
}

func deleteKey(client *clientv3.Client, key string) error {
	_, err := client.Delete(context.Background(), key)
	return err
}

func main() {
	myApp := app.New()
	myWindow := myApp.NewWindow("ETCD DATA")

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: time.Second,
	})
	if err != nil {
		panic(err)
	}

	var data []KV

	detailText := widget.NewMultiLineEntry()
	detailText.SetText("Select An Item From The List")

	list := widget.NewList(
		func() int {
			return len(data)
		},
		func() fyne.CanvasObject {
			return container.NewHBox(widget.NewLabel("Template Object"))
		},
		func(id widget.ListItemID, item fyne.CanvasObject) {
			item.(*fyne.Container).Objects[0].(*widget.Label).SetText(data[id].Key)
		},
	)
	list.OnSelected = func(id widget.ListItemID) {
		detailText.SetText(data[id].Value)
	}
	list.OnUnselected = func(id widget.ListItemID) {
		detailText.SetText("Select An Item From The List")
	}

	refresh := func() {
		data = receiveData(client)
		myWindow.SetTitle("getting data...")
		list.Refresh()
		myWindow.SetTitle("etcd data")
	}

	refresh()
	refreshButton := widget.NewButton("refresh", func() {
		refresh()
	})

	deleteButton := widget.NewButton("delete", func() {
		keyName := widget.NewEntry()
		items := []*widget.FormItem{
			widget.NewFormItem("KeyName", keyName),
		}
		dialog.ShowForm("delete...", "Delete", "Cancel", items, func(b bool) {
			if b {
				if err := deleteKey(client, keyName.Text); err != nil {
					dialog.NewError(err, myWindow)
					return
				}
				refresh()
				dialog.NewInformation("information", fmt.Sprintf("delete done %s", keyName.Text), myWindow).Show()
			}

		}, myWindow)
	})

	top := container.NewHBox(refreshButton, deleteButton)
	bottom := container.NewHSplit(list, detailText)
	main := container.NewVSplit(top, bottom)
	main.SetOffset(0)
	myWindow.SetContent(main)
	fyne.CurrentApp().Settings().SetTheme(theme.LightTheme())
	myWindow.Resize(fyne.NewSize(600, 400))

	myWindow.ShowAndRun()
}

//mainly copy from pm.go
func parseParts(kv *mvccpb.KeyValue) KV {
	parse := func(s string) (uint64, string, error) {
		parts := strings.Split(s, "/")
		if len(parts) != 3 {
			return 0, "", errors.New("len(parts) not 3")
		}
		id, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, "", err
		}

		return id, parts[2], nil
	}

	_, suffix, err := parse(string(kv.Key))
	if err != nil {
		panic(err)
	}

	var ret KV

	switch suffix {
	case "blobStreams":
		var blobs pspb.BlobStreams
		if err := blobs.Unmarshal(kv.Value); err != nil {
			panic(err)
		}
		ret.Key = string(kv.Key)
		ret.Value = fmt.Sprintf("%+v", blobs)
	case "logStream":
		ret.Key = string(kv.Key)
		ret.Value = fmt.Sprintf("%+v", binary.BigEndian.Uint64(kv.Value))
	case "rowStream":
		ret.Key = string(kv.Key)
		ret.Value = fmt.Sprintf("%+v", binary.BigEndian.Uint64(kv.Value))
	case "tables":
		var tables pspb.TableLocations
		if err = tables.Unmarshal(kv.Value); err != nil {
			panic(err)
		}
		ret.Key = string(kv.Key)
		ret.Value = fmt.Sprintf("%+v", tables.Locs)
	case "discard":
		ret.Key = string(kv.Key)
		ret.Value = fmt.Sprintf("%+v", kv.Value)
	case "parent":
		ret.Key = string(kv.Key)
		ret.Value = fmt.Sprintf("%+v", binary.BigEndian.Uint64(kv.Value))
	case "range":
		var rg pspb.Range
		if err = rg.Unmarshal(kv.Value); err != nil {
			panic(err)
		}
		ret.Key = string(kv.Key)
		ret.Value = fmt.Sprintf("%+v", rg)
	default:
		panic("can not parse")
	}

	return ret

}
