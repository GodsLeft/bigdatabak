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
    spark-submit \
        --class anomalydetection \
        --master local[1] \
        --executor-memory 10G \
        sparktest_2.10-1.0.jar
}
runano
# time runkmeans