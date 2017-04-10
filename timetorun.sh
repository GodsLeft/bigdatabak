#!/usr/bin/env bash

# 暂时先这样写
while [ true ];do
    sleep 600
    ppnum=$(ps aux | grep "runspark" | wc -l)
    if [ $ppnum -lt 2 ];then
        mv kmeanstime kmeanstime00
        ./runspark.sh
        echo "done"
        break
    fi
done
# 可以让我在半夜监控一个任务完成，并执行一个任务
#while [ true ];do
#    sleep 10
#    ppnum=$(ps aux | grep "runspark" | wc -l)
#    if [ $ppnum -lt 2 ];then
#        mv kmeanstime kmeanstime00
#        sed -i "s/\$hdfspath/\$hdfspath/g" runkmeanstest.sh  # 切换任务的命令
#        sed -i "s/hadoop fs/#hadoop fs/g" runkmeanstest.sh
#        sed -i "s/#alluxio/alluxio/g" runkmeanstest.sh
#        #sed -i "s/hadoop fs -rm -R kmeans/alluxio fs rm -R \/user\/bigdata\/kmeans/g" timetorun.sh
#        ./runkmeanstest
#        #./runspark.sh
#        echo "hello"
#        break
#    fi
#done