#!/bin/bash

base_addr=$1
num_workers=$2
server_ip=$3

echo "starting $num_workers workers starting from 127.23.27.$base_addr"
export SPARK_HOME=/home/ikt/sparkhome

for i in `seq $base_addr $(($base_addr+$num_workers-1))`;
do
	echo 172.23.27.$i
	docker run -d --privileged -v $SPARK_HOME:/opt/spark --net=host spark-test-worker --ip 172.23.27.$i spark://$server_ip:7077
done

