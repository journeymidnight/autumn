all:
	go build
	@if [ ! -d store1 ]; then mkdir store1 store2 store3 store4; fi
	@bash -c 'mkdir  -p store1/{sda,sdb,wal} store2/{sda,sdb,wal} store3/{sda,sdb,wal} store4/{sdc,wal}'
format:
	../autumn-client/autumn-client format --walDir store1/wal --listenUrl 127.0.0.1:4001 store1/sda store1/sdb
	../autumn-client/autumn-client format --walDir store2/wal --listenUrl 127.0.0.1:4002 store2/sda store2/sdb
	../autumn-client/autumn-client format --walDir store3/wal --listenUrl 127.0.0.1:4003 store3/sda store3/sdb
	../autumn-client/autumn-client format --walDir store4/wal --listenUrl 127.0.0.1:4004 store4/sdc
clean:
	rm -rf *.log
	rm -rf *.toml
	rm -rf store1 store2 store3 store4
