#!/usr/bin/env bash

CURRPATH=$(cd `dirname $0`; pwd)
BASEPATH=$CURRPATH/../../../..

MASTERPATH=spark://master:7077

runwordcount(){
    spark-submit \
        --class wordcount.wordcount \
        --master $MASTERPATH \
        --executor-memory 20G \
        $BASEPATH/target/scala-2.10/sparktest*.jar \
        hdfs://slave04:9000/user/bigdata/ips.csv
}

time -p runwordcount