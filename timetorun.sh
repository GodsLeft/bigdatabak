#!/usr/bin/env bash

hdfspath=hdfs://master:9000/user/bigdata
allupath=alluxio://master:19998/user/bigdata

runkmeanstest(){
    for index in {0..5};do
        echo "===="$index"====" >> kmeanstime
        hdfsinput=$hdfspath/ipsdata/ips_$index.csv
        for cnt in {0..9};do
            hadoop fs -rm -R kmeans
            #alluxio fs rm -R /user/bigdata/kmeans
            hdfsout=$hdfspath/kmeans
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
# 可以让我在半夜执行一个任务
while [ true ];do
    sleep 10
    ppnum=$(ps aux | grep "runspark" | wc -l)
    if [ $ppnum -ge 2 ];then
        #sed -i ""  # 切换任务的命令
        #./runspark.sh
        echo "hello"
        break
    fi
done