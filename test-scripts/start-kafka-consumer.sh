#!/bin/bash

TOPIC=$1

IP=`docker inspect kixidatastore_zookeeper_1  | jq '.[].NetworkSettings.Networks.bridge.IPAddress'`

echo $IP

docker run --rm ches/kafka kafka-console-consumer.sh --topic $TOPIC --from-beginning --zookeeper $IP:2181
