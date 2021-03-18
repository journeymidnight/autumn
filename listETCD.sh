ETCDCTL_API=3 etcdctl get "" --prefix --keys-only|sed '/^$/d'|sort
