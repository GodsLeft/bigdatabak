#!/usr/bin/env bash

runcache(){

    {
        time -p spark-submit --class alluxiostore.sparkcache \
                            --master spark://master:7077 \
                            --executor-memory 20G \
                            sparktest*.jar \
                            ipsdata/ips_5.csv
    }
}

runalluxio(){
    {
        time -p spark-submit --class alluxiostore.alluxiostore \
                            --master spark://master:7077 \
                            --executor-memory 20G \
                            sparktest*.jar \
                            ipsdata/ips_5.csv
    }
}

runcache
runalluxio