#!/usr/bin/env bash

CURRPATH=$(cd `dirname $0`; pwd)
BASEPATH=$CURRPATH/../../../..

runwordcount(){
    spark-submit \
        --class wordcount.wordcount \
        --master spark://slave04:7077 \
        --executor-memory 20G \
        target/scala-2.10/sparktest*.jar \
        hdfs://slave04:9000/user/bigdata/ips.csv
}