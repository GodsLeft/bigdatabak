#!/usr/bin/env bash

makedata(){
    hadoop fs -rm -R skewdata1
    hadoop fs -rm -R skewdata2
    spark-submit --class dataskew.makedataskew \
                    --master spark://master:7077 \
                    --executor-memory 20G \
                    sparktest*.jar \
                    0.01 \
                    skewdata1


    spark-submit --class dataskew.makedataskew \
                    --master spark://master:7077 \
                    --executor-memory 20G \
                    sparktest*.jar \
                    0.5 \
                    skewdata2
}

maketest(){
    echo "====  unbalance  ===="
    hadoop fs -rm -R skewdataout1 >/dev/null
    hadoop fs -rm -R skewdataout2 >/dev/null
    {
        time -p spark-submit --class dataskew.groupbykey \
                    --master spark://master:7077 \
                    --executor-memory 20G \
                    sparktest*.jar \
                    skewdata1 \
                    skewdataout1 #2> /dev/null
    }

    echo ""
    echo "====  balance  ===="
    {
        time -p spark-submit --class dataskew.groupbykey \
                            --master spark://master:7077 \
                            --executor-memory 20G \
                            sparktest*.jar \
                            skewdata2 \
                            skewdataout2 #2> /dev/null
    }
}

makedata
maketest
