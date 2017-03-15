#!/usr/bin/env bash

runwordcount(){
    spark-submit \
        --class wordcount \
        --master spark://master:7077 \
        --executor-memory 20G \
        sparktest_2.10-1.0.jar \
        hdfs://master:9000/user/bigdata/ips.csv \
        alluxio://master:19998/user/bigdata/ips.csv
}

runkmeans(){
    hadoop fs -rm -R kmeans
    spark-submit \
        --class kmeanstest \
        --master spark://master:7077 \
        --executor-memory 20G \
        sparktest_2.10-1.0.jar
}

runano(){
    hadoop fs -rm -R yichang
    spark-submit \
        --class anomalydetection \
        --master spark://master:7077 \
        --executor-memory 20G \
        sparktest_2.10-1.0.jar
}

# 验证一些问题的时候使用
runsomeidea(){
    spark-submit \
        --class someidea \
        --master local[*] \
        --executor-memory 2G \
        sparktest_2.10-1.0.jar
}

# time runkmeans
time runano > logstdout 2> logstderr &
# runsomeidea