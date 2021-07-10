all:etcdviewer rs_reconstruct
rs_reconstruct:rs_reconstruct.go
	go build $<
etcdviewer:etcdviewer.go
	go build $<
clean:
	rm etcdviewer
	rm rs_reconstruct
