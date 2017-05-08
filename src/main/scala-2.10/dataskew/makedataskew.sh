#!/usr/bin/env bash

makedata(){
    hadoop fs -rm -R skewdata*
    spark-submit --class dataskew.makedataskew \
                    --master spark://master:7077 \
                    --executor-memory 20G \
                    sparktest*.jar \
                    0.1 \
                    skewdata1


    spark-submit --class dataskew.makedataskew \
                    --master spark://master:7077 \
                    --executor-memory 20G \
                    sparktest*.jar \
                    0.5 \
                    skewdata2
}

maketest(){
    echo "====start===="
    { time -p spark-submit --class dataskew.groupbykey \
                    --master spark://master:7077 \
                    --executor-memory 20G \
                    sparktest*.jar \
                    skewdata1 \
                    skewdataout2
    }

    {
        time -p spark-submit --class dataskew.groupbykey \
                            --master spark://master:7077 \
                            --executor-memory 20G \
                            sparktest*.jar \
                            skewdata2 \
                            skewdataout2
    }
}

makedata
maketest
