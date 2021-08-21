mkdir -p store/sda
if [ -f config.toml ]; then
    ./extent-node --config config.toml
else
    while true
    do
	    ./autumn-client format --output config.toml --advertise-url $(hostname):4001 --listen-url :4001 --etcd-urls stream-manager:2379 --sm-urls stream-manager:3401 store/sda
        if [ $? -eq "0" ];then
            break
        fi
        sleep 1
    done
    ./extent-node --config config.toml
fi
