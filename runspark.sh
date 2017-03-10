#!/usr/bin/env bash
spark-submit \
    --class sparktest \
    --master spark://master:7077 \
    --executor-memory 20G \
    sparktest_2.10-1.0.jar \
    hdfs://master:9000/user/bigdata/ips.csv \
    alluxio://master:19998/user/bigdata/ips.csv