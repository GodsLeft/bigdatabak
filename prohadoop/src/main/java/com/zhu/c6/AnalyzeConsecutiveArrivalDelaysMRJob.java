package com.zhu.c6;

import java.io.IOException;
import com.zhu.utils.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class AnalyzeConsecutiveArrivalDelaysMRJob extends Configured implements Tool{
    public static class ArrivalFlightKeyBasedPartioner
            extends Partitioner<ArrivalFlightKey, Text>{
        @Override
        public int getPartition(ArrivalFlightKey key, Text value, int numPartitions){
            return Math.abs(key.destinationAirport.hashCode() % numPartitions);
        }
    }

    public static class AnalyzeConsecutiveDelaysMapper
            extends Mapper<LongWritable, Text, ArrivalFlightKey, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            if(!AirlineDataUtils.isHeader(value)){
                String[] contents = value.toString().split(",");
                String arrivingAirport = AirlineDataUtils.getDestination(contents);
                String arrivingDtTime = AirlineDataUtils.getArrivalTime(contents);

                int arrivalDelay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getArrivalDelay(contents), 0);
                if(arrivalDelay > 0){
                    ArrivalFlightKey afKey = new ArrivalFlightKey(new Text(arrivingAirport), new Text(arrivingDtTime));
                    context.write(afKey, value);
                }
            }
        }
    }

    public static class AnalyzeConsecutiveDelaysReducer extends Reducer<ArrivalFlightKey, Text, NullWritable, Text>{
        public void reduce(ArrivalFlightKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

            Text previousRecord = null;
            for(Text v : values){
                StringBuilder out = new StringBuilder("");
                if(previousRecord == null){
                    out.append(v.toString()).append("|");
                }else{
                    out.append(v.toString()).append("|").append(previousRecord.toString());
                }
                context.write(NullWritable.get(), new Text(out.toString()));
                previousRecord = new Text(v.toString());
            }
        }
    }

    public int run(String[] allArgs) throws Exception{
        Job job = Job.getInstance(getConf());
        job.setJarByClass(AnalyzeConsecutiveArrivalDelaysMRJob.class);

        job.setMapperClass(AnalyzeConsecutiveDelaysMapper.class);
        job.setReducerClass(AnalyzeConsecutiveDelaysReducer.class);
        job.setPartitionerClass(ArrivalFlightKeyBasedPartioner.class);
        job.setSortComparatorClass(ArrivalFlightKeySortingComparator.class);
        job.setGroupingComparatorClass(ArrivalFlightKeyGroupingComparator.class);

        job.setMapOutputKeyClass(ArrivalFlightKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

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
        ToolRunner.run(new AnalyzeConsecutiveArrivalDelaysMRJob(), args);
    }
}

