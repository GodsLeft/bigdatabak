package com.zhu;

import java.text.DecimalFormat;
import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import com.zhu.utils.AirlineDataUtils;

public class SplitByMonthMRJob extends Configured implements Tool{

    public static class SplitByMonthMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            if(!AirlineDataUtils.isHeader(value)){
                int month = Integer.parseInt(AirlineDataUtils.getMonth(value.toString().split(",")));
                context.write(new IntWritable(month), value);
            }
        }
    }

    public static class SortByMonthAndDayOfWeekReducer extends Reducer<IntWritable, Text, NullWritable, Text>{
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            for(Text output : values){
                context.write(NullWritable.get(), new Text(output.toString()));
            }
       }
    }

    public static class MonthPartioner extends Partitioner<IntWritable, Text>{
        @Override
        public int getPartition(IntWritable month, Text value, int numPartitions){
            return (month.get() - 1);
        }
    }

    public int run(String[] allArgs) throws Exception{
        Job job = Job.getInstance(getConf());
        job.setJarByClass(SplitByMonthMRJob.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(SplitByMonthMapper.class);
        job.setReducerClass(SortByMonthAndDayOfWeekReducer.class);
        job.setPartitionerClass(MonthPartioner.class);
        job.setNumReduceTasks(12);
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        if(status)
            return 0;
        else
            return 1;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        ToolRunner.run(new SplitByMonthMRJob(), args);
    }
}
