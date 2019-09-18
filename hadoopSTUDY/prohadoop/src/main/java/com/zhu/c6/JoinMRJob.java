package com.zhu.c6;

import com.zhu.utils.*;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

public class JoinMRJob extends Configured implements Tool{
    public static class CarrierMasterMapper extends Mapper<LongWritable, Text, CarrierKey, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            if(!AirlineDataUtils.isCarrierFileHeader(value.toString())){
                String[] carrierDetails = AirlineDataUtils.parseCarrierLine(value.toString());
                Text carrierCode = new Text(carrierDetails[0].toLowerCase().trim());
                Text desc = new Text(carrierDetails[1].toUpperCase().trim());
                CarrierKey ck = new CarrierKey(CarrierKey.TYPE_CARRIER, carrierCode, desc);
                context.write(ck, new Text());
            }
        }
    }

    public static class FlightDataMapper extends Mapper<LongWritable, Text, CarrierKey, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            if(!AirlineDataUtils.isHeader(value)){
                String[] contents = value.toString().split(",");
                String carrierCode = AirlineDataUtils.getUniqueCarrier(contents);
                Text code = new Text(carrierCode.toLowerCase().trim());
                CarrierKey ck = new CarrierKey(CarrierKey.TYPE_DATA, code);

                DelaysWritable dw = AirlineDataUtils.parseDelaysWritable(value.toString());
                Text dwTxt = AirlineDataUtils.parseDelaysWritableToText(dw);
                context.write(ck, dwTxt);
            }
        }
    }

    public static class CarrierCodeBasedPartioner extends Partitioner<CarrierKey, Text>{
        @Override
        public int getPartition(CarrierKey key, Text value, int numPartitions){
            return Math.abs(key.code.hashCode() % numPartitions);
        }
    }

    public static class JoinReducer extends Reducer<CarrierKey, Text, NullWritable, Text>{
        public void reduce(CarrierKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String carrierDesc = "UNKNOWN";
            for(Text v:values){
                if(key.type.equals(CarrierKey.TYPE_CARRIER)){
                    carrierDesc = key.desc.toString();
                    continue;
                }else{
                    Text out = new Text(v.toString()+","+carrierDesc);
                    context.write(NullWritable.get(), out);
                }
            }
        }
    }

    public int run(String[] allArgs) throws Exception{
        Job job = Job.getInstance(getConf());
        job.setJarByClass(JoinMRJob.class);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CarrierMasterMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapOutputKeyClass(CarrierKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(CarrierSortComparator.class);
        job.setGroupingComparatorClass(CarrierGroupComparator.class);
        job.setReducerClass(JoinReducer.class);
        job.setPartitionerClass(CarrierCodeBasedPartioner.class);

        boolean status = job.waitForCompletion(true);
        if(status){
            return 0;
        }else{
            return 1;
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        ToolRunner.run(new JoinMRJob(), args);
    }
}
