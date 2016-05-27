#!/bin/bash
hadoop fs -rm -R output
#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.SelectClauseMRJob sampledata output
#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.WhereClauseMRJob -D map.where.delay=10 sampledata output
#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.AggregationMRJob sampledata output
#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.AggregationWithCombinerMRJob sampledata output
#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.SplitByMonthMRJob sampledata output
#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.c6.SortAscMonthDescWeekMRJob -D mapred.reduce.tasks=12 sampledata output
#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.c6.AnalyzeConsecutiveArrivalDelaysMRJob sampledata output
#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.c6.JoinMRJob -D mapred.reduce.tasks=12 sampledata masterdata/carriers.csv output
#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.c6.MapSideJoinMRJob \
#    sampledata \
#    output \
#    src/main/resources/masterdata/carriers.csv \
#    src/main/resources/masterdata/airports.csv

#hadoop jar target/prohadoop-1.0-SNAPSHOT.jar com.zhu.c7.TextToXMLConversionJob \
#    -D mapreduce.output.basename=airlinedata \
#    sampledata \
#    output

#java -cp target/prohadoop-1.0-SNAPSHOT.jar com.zhu.c10.HiveJdbcClient

hadoop jar $HADOOP_HOME/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-2.7.2.jar Client \
    -classpath target/prohadoop-1.0-SNAPSHOT.jar \
    -cmd "java com.zhu.c17.ApplicationMaster c17/PatentList1.txt c17/patents"
