#!/bin/bash
hadoop jar /home/hadoop/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar \
	-D mapred.reduce.tasks=0 \
	-input zhu_disk/output_streaming_shell/part-00000 \
	-output zhu_disk/output_streaming_shell_2 \
	-mapper 'wc -l' 
#	-Dmapred.reduce.tasks=0
