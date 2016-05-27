
hadoop jar /home/hadoop/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar \
		-D mapred.reduce.tasks=1 \
		-input zhu_disk/inputCite/apat63_99.txt \
		-output zhu_disk/output_streaming_Attr \
		-mapper 'AttributeMax.py 8' \
		-file AttributeMax.py
