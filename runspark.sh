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
        --class kmeans \
        --master spark://master:7077 \
        --executor-memory 20G \
        sparktest_2.10-1.0.jar
}

# 整个数据集上的异常检测算法
runano(){
    hadoop fs -rm -R yichang
    alluxio fs rm -R yichang
    spark-submit \
        --class anomalydetection \
        --master spark://master:7077 \
        --executor-memory 20G \
        sparktest_2.10-1.0.jar \
        hdfs://master:9000/user/bigdata/ips.csv \
        hdfs://master:9000/user/bigdata/yichang

    rm yichang -r
    hadoop fs -get yichang
    alluxio fs copyToLocal /user/bigdata/yichang yichang
}

# 验证一些问题的时候使用
runsomeidea(){
    spark-submit \
        --class someidea \
        --master local[*] \
        --executor-memory 2G \
        sparktest_2.10-1.0.jar
}

runanom(){
    hadoop fs -rm -R yichang
    alluxio fs rm -R /user/bigdata/yichang
    spark-submit \
        --class anomalydetection \
        --master spark://master:7077 \
        --executor-memory 20G \
        sparktest_2.10-1.0.jar \
        $1 \
        $2
    #rm yichang -r
    #hadoop fs -get yichang
    #alluxio fs copyToLocal /user/bigdata/yichang yichang
}

runfpg(){
    spark-submit \
        --class fpgrowth \
        --master spark://master:7077 \
        --executor-memory 20G \
        sparktest_2.10-1.0.jar
}

runtfidf(){
    spark-submit \
        --class tfidf \
        --master spark://master:7077 \
        --executor-memory 20G \
        sparktest_2.10-1.0.jar
}

hdfspath=hdfs://master:9000/user/bigdata
allupath=alluxio://master:19998/user/bigdata

runwordcounttest(){
    for index in {0..5};do
        #hdfsinput=$hdfspath/ipsdata/ips_$index.csv
        hdfsinput=$allupath/ipsdata/ips_$index.csv
        echo "===="$index"====" >> wordcounttime
        for cnt in {0..9};do
            { time -p spark-submit \
                    --class wordcount \
                    --master spark://master:7077 \
                    --executor-memory 20G \
                    sparktest*.jar $hdfsinput 2> /dev/null;
            } 2>> wordcounttime
        done
    done
}

runkmeanstest(){
    for index in {0..5};do
        echo "===="$index"====" >> kmeanstime
        #hdfsinput=$hdfspath/ipsdata/ips_$index.csv
        hdfsinput=$allupath/ipsdata/ips_$index.csv
        for cnt in {0..9};do
            #hadoop fs -rm -R kmeans
            #hdfsout=$hdfspath/kmeans
            alluxio fs rm -R /user/bigdata/kmeans
            hdfsout=$allupath/kmeans
            { time -p spark-submit \
                        --class kmeans \
                        --master spark://master:7077 \
                        --executor-memory 20G \
                        sparktest*.jar \
                        $hdfsinput \
                        $hdfsout 2> /dev/null;
            } 2>> kmeanstime
        done
    done
}

runanotest(){
    #hdfsout=$hdfspath/yichang
    hdfsout=$allupath/yichang
    #alluout=alluxio://master:19998/user/bigdata/yichang

    for index in {0..5};do
        echo "===="$index"====" >> anotime
        for cnt in {0..9};do
            #hdfsinput=$hdfspath/ipsdata/ips_$index.csv
            hdfsinput=$allupath/ipsdata/ips_$index.csv
            #hadoop fs -rm -R $hdfsout
            alluxio fs rm -R $hdfsout
            #{ time runanom $hdfsinput $hdfsout > hdfsAnoout 2> /dev/null; } 2>> anotime
            { time -p spark-submit \
                        --class anomalydetection \
                        --master spark://master:7077 \
                        --executor-memory 20G \
                        sparktest*.jar \
                        $hdfsinput \
                        $hdfsout \
                        2>/dev/null;
            } 2>> anotime
        done
    done
}

runanotest_yarn(){
    hdfsout=$hdfspath/yichang
    for index in {0..1};do
        echo "===="$index"====" >> anotimeyarn
        hdfsinput=$hdfspath/ipsdata/ips_$index.csv
        for cnt in {0..1};do
            hadoop fs -rm -R $hdfsout
            { time -p spark-submit \
                        --class anomalydetection \
                        --master yarn \
                        --num-executors 72 \
                        --driver-memory 5G \
                        --executor-memory 10G \
                        sparktest*.jar \
                        $hdfsinput \
                        $hdfsout \
                        2> /dev/null;
            } 2>> anotimeyarn
        done
    done
}

runsrcdstip(){
    # hadoop操作
    hadoop fs -rm -R srcdstip
    spark-submit --class someidea.srcdstip --master spark://master:7077 --executor-memory 20G sparktest*.jar

    # 本地操作
    rm -r srcdstip
    hadoop fs -get srcdstip
    cd srcdstip
    mv part-00000 srcdst.dot
    sed -i '1i\digraph srcdst{' srcdst.dot
    echo '}' >> srcdst.dot
    dot -Tjpg srcdst.dot -o srcdst.jpg
}

mkdot(){
    mv part-00000 srcdst.dot
    sed -i '1i\digraph srcdst{' srcdst.dot
    echo '}' >> srcdst.dot
    dot -Tjpg srcdst.dot -o srcdst.jpg
}

runstreamingdemo(){
    spark-submit \
        --class someidea.streamingdemo \
        sparktest*.jar 2> /dev/null
}

runstreaming(){
    # 这样做有点问题，不能够终结此程序
    ./streamingmock.sh &    #向本机的9999端口发送数据
    spark-submit --class someidea.streamingdemo sparktest*.jar 2> /dev/null &
    sleep 5
    ./todot.sh &
}
#time runtfidf 2> /dev/null
#time runwordcount
#time runkmeans
#time runano > stdout 2> stderr &
#runsomeidea
#time runanotest
runkmeanstest
#runwordcounttest
#runanotest
#runsrcdstip
#runstreaming

# 程序中的输出使用输出重定向
# Spark的输出使用错误重定向
# alluxio fs free /user/bigdata/ipsdata