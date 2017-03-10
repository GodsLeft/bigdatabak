#!/usr/bin/env bash
sbt package

scp -P 13000 ./runspark.sh ./target/scala-2.10/sparktest_2.10-1.0.jar bigdata@slave04:~/
# scp -P 13000 ./runspark.sh bigdata@slave04:~/

#ssh -p 13000 bigdata@slave04 "spark-submit \
#                            --class sparktest \
#                            --master spark://master:7077 \
#                            sparktest_2.10-1.0.jar"
