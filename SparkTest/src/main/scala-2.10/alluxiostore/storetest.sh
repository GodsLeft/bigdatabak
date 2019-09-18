#!/usr/bin/env bash
#--conf "spark.driver.extraJavaOptions=-Dcom.sun.management.jmxremote.port=7091 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false" \
#--conf "spark.executor.extraJavaOptions=-Dcom.sun.management.jmxremote.port=7092 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false" \
runcache(){
    for index in {0..5};do
        hdfsinput="ipsdata/ips_"$index.csv
        echo "===="$index"====" >> sparkcachetime

        {
            time -p spark-submit --class alluxiostore.sparkcache \
                            --master spark://master:7077 \
                            --executor-memory 20G \
                            sparktest*.jar \
                            $hdfsinput  2> /dev/null
        } 2>> sparkcachetime
    done
}

runalluxio(){
    for index in {0..5};do
        hdfsinput="ipsdata/ips_"$index.csv
        linshi="alluxio://master:19998/user/bigdata/linshi"
        alluxio fs rm -R $linshi
        echo "===="$index"====" >> alluxiostoretime

        {
            time -p spark-submit --class alluxiostore.alluxiostore \
                            --master spark://master:7077 \
                            --executor-memory 20G \
                            sparktest*.jar \
                            $hdfsinput \
                            $linshi  2>/dev/null
        } 2>> alluxiostoretime
    done
}

runcache
#runalluxio