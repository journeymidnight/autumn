all:
	go build
	@if [ ! -d store1 ]; then mkdir store1 store2 store3 store4; fi
	@bash -c 'mkdir  -p store1/{sda,sdb,wal} store2/{sda,sdb,wal} store3/{sda,sdb,wal} store4/{sdc,wal}'
format:
	../autumn-client/autumn-client format --sm-urls 127.0.0.1:3401,127.0.0.1:3402,127.0.0.1:3403 --etcd-urls 127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381 --output en_1.toml --waldir store1/wal --advertise-url 127.0.0.1:4001 --listen-url :4001 store1/sda store1/sdb 
	../autumn-client/autumn-client format --sm-urls 127.0.0.1:3401,127.0.0.1:3402,127.0.0.1:3403 --etcd-urls 127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381 --output en_4.toml --waldir store2/wal --advertise-url 127.0.0.1:4002 --listen-url :4002 store2/sda store2/sdb 
	../autumn-client/autumn-client format --sm-urls 127.0.0.1:3401,127.0.0.1:3402,127.0.0.1:3403 --etcd-urls 127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381 --output en_7.toml --waldir store3/wal --advertise-url 127.0.0.1:4003 --listen-url :4003 store3/sda store3/sdb 
	../autumn-client/autumn-client format --sm-urls 127.0.0.1:3401,127.0.0.1:3402,127.0.0.1:3403 --etcd-urls 127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381 --output en_10.toml --waldir store4/wal --advertise-url 127.0.0.1:4004 --listen-url :4004 store4/sdc 
clean:
	rm -rf *.log
	rm -rf *.toml
	rm -rf store1 store2 store3 store4
