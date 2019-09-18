#!/usr/bin/env bash

# 这个文件只是为了在别的机器上显示图片
while [ true ];do
    sleep 0.1
    nc -l 20000 > srcdst.png
done
