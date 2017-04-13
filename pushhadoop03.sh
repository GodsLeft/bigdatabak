#!/usr/bin/env bash

scp -P xxxx ./runspark.sh \
             ./cutdata.sh \
             ./todot.sh \
             ./streamingmock.sh \
             ./tocsv.sh \
             ./target/scala-2.10/sparktest_2.10-1.0.jar \
             xxxx@xxxxxxxx:~/sparktest/
