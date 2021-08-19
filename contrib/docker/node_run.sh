mkdir -p store/sda
if [ -f en_1.toml ]; then
    ./extent-node --config en_1.toml
else
    while true
    do
        sleep 1
        ./autumn-client format --listenUrl extent-node:4001 --etcdAddr stream-manager:2379 --smAddr stream-manager:3401 store/sda
        if [ $? -eq "0" ];then
            break
        fi
    done
    ./extent-node --config en_1.toml
fi