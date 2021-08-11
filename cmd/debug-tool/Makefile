all:etcdviewer rs_reconstruct valid_blocks cp_extent
cp_extent:cp_extent.go
	go build $<
valid_blocks:valid_blocks.go
	go build $<
rs_reconstruct:rs_reconstruct.go
	go build $<
etcdviewer:etcdviewer.go
	go build $<
clean:
	rm etcdviewer
	rm rs_reconstruct
	rm valid_blocks
	rm cp_extent
