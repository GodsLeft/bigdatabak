#!/usr/bin/env bash

sbt package

# 上传到slave04
scp -r -P 13000 ../SparkTest bigdata@slave04:~/alldabian/
