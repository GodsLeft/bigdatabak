#!/usr/bin/env bash

sbt package

#expect -c '''
#spawn scp -P 40000 \
#             ./startsparktest.sh \
#             ./cutdata.sh \
#             ./src/main/scala-2.10/streaming/todot.sh \
#             ./src/main/scala-2.10/streaming/streamingmock.sh \
#             ./src/main/scala-2.10/dataskew/makedataskew.sh \
#             ./src/main/scala-2.10/alluxiostore/storetest.sh \
#             ./tocsv.sh \
#             ./target/scala-2.10/sparktest_2.10-1.0.jar \
#             bigdata@slave04:~/sparktest/
#
#expect "*assword:*"
#send "zhu@hadoop\n"
#interact
#'''

#ssh -p 13000 bigdata@slave04 "spark-submit \
#                            --class wordcount \
#                            --master spark://master:7077 \
#                            sparktest_2.10-1.0.jar"

# 上传到slave04
scp -r ../SparkTest bigdata@slave04:~/alldabian/
