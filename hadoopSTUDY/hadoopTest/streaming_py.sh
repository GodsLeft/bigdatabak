
hadoop jar /home/hadoop/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar \
		-D mapred.reduce.tasks=1 \
		-input zhu_disk/inputCite/cite75_99 \
		-output zhu_disk/output_streaming_py \
		-mapper 'RandomSample.py 10' \
		-file RandomSample.py
