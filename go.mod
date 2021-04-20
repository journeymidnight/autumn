module github.com/journeymidnight/autumn

go 1.14

require (
	fyne.io/fyne v1.4.3
	fyne.io/fyne/v2 v2.0.1
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/cespare/xxhash v1.1.0
	github.com/coreos/bbolt v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/etcd v3.3.22+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dgraph-io/ristretto v0.0.3
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eapache/queue v1.1.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.5
	github.com/golang/snappy v0.0.2
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.2 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.2 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.15.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/pkg/errors v0.9.1
	github.com/pkg/xattr v0.4.1
	github.com/prometheus/client_golang v1.5.1 // indirect
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/tmc/grpc-websocket-proxy v0.0.0-20200427203606-3cfed13b9966 // indirect
	github.com/urfave/cli v1.22.5 // indirect
	github.com/urfave/cli/v2 v2.3.0
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/etcd v3.3.25+incompatible
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20201021035429-f5854403a974
	golang.org/x/sys v0.0.0-20210119212857-b64e53b001e4
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e // indirect
	golang.org/x/tools v0.1.0 // indirect
	google.golang.org/grpc v1.33.0
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5

replace google.golang.org/grpc => google.golang.org/grpc v1.23.0
