
hadoop jar /home/hadoop/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input zhu_disk/inputCite/cite75_99 -output zhu_disk/output_streaming_shell -mapper 'cut -f 2 -d ,' -reducer 'uniq'
