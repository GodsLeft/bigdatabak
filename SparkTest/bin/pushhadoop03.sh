#!/usr/bin/env bash

# 一些上传用的脚本
scp -P xxxx ./runspark.sh \
             ./cutdata.sh \
             ./todot.sh \
             ./streamingmock.sh \
             ./tocsv.sh \
             ./target/scala-2.10/sparktest_2.10-1.0.jar \
             xxxx@xxxxxxxx:~/sparktest/

# 自动输入密码
expect -c '''
spawn scp -P 40000 \
    ././../../SparkTest/* \
    bigdata@slave04:~/sparktest

expect "*assword:*"
send "password"
interact
'''


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
#send "xxxxxxxxxx\n"
#interact
#'''