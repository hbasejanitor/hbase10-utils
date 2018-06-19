#!/bin/bash
CMD=$1

export AUTO_PEER =""
export KAFKA_PROPS_FILE=""

if [[ "$PROXY_AUTO_PEER" -ne "" ]] ;
then
    export AUTO_PEER = " -a "
fi

if [[ "$PROXY_PEER_ID" -ne "" ]] ;
then
    export PEER_ID = " -p $PROXY_PEER_ID "
else
    echo "HBase peer id is required"
    exit -1
fi

if [[ "$KAFKA_BROKERS" -eq "" ]] ;
then
    echo "kafka brokers are required"
    exit -1
else
    export KAFKA_BROKERS=" -b $KAFKA_BROKERS "
fi

if [[ "$KAFKA_CONNECTION_PROPERTIES_FILE" -ne "" ]] ;
then
    export KAFKA_PROPS_FILE = " -f $KAFKA_CONNECTION_PROPERTIES_FILE"
fi

case $CMD in
  (start)
    echo "Starting proxy"
    exec hbase $AUTO_PEER $PEER_ID $KAFKA_BROKERS $KAFKA_PROPS_FILE
    ;;
  (*)
    echo "Don't understand [$CMD]"
    ;;
esac


