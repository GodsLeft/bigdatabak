#!/usr/bin/env bash

# 为了模拟数据流，每秒中输出1行日志文本
# nc -lk 其中的k表示连续监听
while read LINE
do
    sleep 0.1
    echo $LINE
done < ./ips.csv | nc -lk 9999
