#!/usr/bin/env bash
set -eu

create_rem_directory() {
    NAME=$1
    BASE_PORT=$2
    #clear place
    rm -rf $NAME
    mkdir $NAME
    DIR=$(realpath $NAME)

    for SRC in rem start-stop-daemon.py rem-server.py client network_topology.cfg; do
        ln -sf ../$SRC $DIR/$SRC
    done
    cp rem.cfg-default $DIR/rem.cfg
    cp setup_env-default.sh $DIR/setup_env.sh
    sed -ibak "s|/usr/local/rem|$DIR|g" $DIR/rem.cfg
    sed -ibak "/^network_topology/s|.*|network_topology = local://$DIR/network_topology.cfg|g" $DIR/rem.cfg
    sed -ibak "/^network_hostname/s|.*|network_hostname = $NAME|g" $DIR/rem.cfg
    sed -ibak "s|port\s*=\s*\([1-9]*\)|port = ${BASE_PORT}\1|g" $DIR/rem.cfg
    sed -ibak "s|allow_backup_rpc_method\s*=\s*no|allow_backup_rpc_method = yes|g" $DIR/rem.cfg
}

cat <<CONFIG > network_topology.cfg
[servers]
local-01 = http://localhost:18104, http://localhost:18105
local-02 = http://localhost:28104, http://localhost:28105
CONFIG

create_rem_directory "local-01" "1"
create_rem_directory "local-02" "2"
