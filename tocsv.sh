#!/usr/bin/env bash

# 将time产生的文件转化为csv文件
timefile=$1

cat $timefile | grep '=\|\(real\)' > real$timefile

# 将仅包含real行的文件分为多个 linshixx 文件
csplit real$timefile /====/ -n 2 -s {*} -f linshi -b"%02d"
rm linshi00
rm real$timefile

num=$( ls -al ./ | grep 'linshi' | wc -l)

sed -i "1d" linshi*

# 去掉文件的第一列
#for i in {1..6};do
for i in $(seq 1 $num);do
    cut -f 2 -d ' ' linshi0$i > linshi$i
    rm linshi0$i
done

# 按列合并
paste linshi* -d ',' > ${timefile}.csv
rm linshi*