#!/usr/bin/env bash

sbt package

expect -c '''
spawn scp -P 13000 ./runspark.sh \
             ./target/scala-2.10/sparktest_2.10-1.0.jar \
             bigdata@slave04:~/

expect "*assword:*"
send "xxxxxxxx\n"
interact
'''

#ssh -p 13000 bigdata@slave04 "spark-submit \
#                            --class wordcount \
#                            --master spark://master:7077 \
#                            sparktest_2.10-1.0.jar"
