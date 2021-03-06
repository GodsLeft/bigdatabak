#!/usr/bin/env bash

# 1M / 1637行
nums=1636
if [ ! -d "ipsdata" ];then
    mkdir ipsdata
fi

head -$[ $nums * 100 ] ips.csv > ./ipsdata/ips_0.csv &
head -$[ $nums * 512 ] ips.csv > ./ipsdata/ips_1.csv &
for index in {1..5};do
    lines=$[ index * $nums * 1024 ]
    head -$lines ips.csv > ./ipsdata/ips_$[ $index + 1 ].csv &
done
wait