package com.zhu.c6;
import com.zhu.utils.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
public class MapSideJoinMRJob extends Configured implements Tool{
    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
        private Map<String, String[]> airports = new HashMap<String, String[]>();
        private Map<String, String[]> carriers = new HashMap<String, String[]>();

        private void readAirport(URI uri) throws Exception{
            List<String> lines = FileUtils.readLines(new File(uri));
            for(String tmp : lines){
                airports.put(tmp.split(",")[0], tmp.split(","));
            }
        }
        private void readCarriers(URI uri) throws Exception{
            List<String> lines = FileUtils.readLines(new File(uri));
            for(String tmp : lines){
                carriers.put(tmp.split(",")[0], tmp.split(","));
            }
        }

        public void setup(Context context) throws IOException, InterruptedException{
            try{
                URI[] uris = context.getCacheFiles();
                for(URI uri:uris){
                    if(uri.toString().endsWith("airports.csv")){
                        this.readAirport(uri);
                    }
                    if(uri.toString().endsWith("carriers.csv")){
                        this.readCarriers(uri);
                    }
                }
            }catch(Exception ex){
                throw new RuntimeException(ex);
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            if(!AirlineDataUtils.isHeader(value)){
                DelaysWritable dw = AirlineDataUtils.parseDelaysWritable(value.toString());
                String originAirportCd = dw.originAirportCode.toString();
                String destAirportCd = dw.destAirportCode.toString();
                String carrierCd = dw.carrierCode.toString();

                String[] originAirport = this.airports.get(originAirportCd);
                String[] destAirport = this.airports.get(destAirportCd);
                String[] carrier = this.carriers.get(carrierCd);

                String originAirportDesc = "";
                if(originAirport != null)
                    originAirportDesc = originAirport[1].replaceAll(",","");

                String destAirportDesc = "";
                if(destAirport != null)
                    destAirportDesc = destAirport[1].replaceAll(",","");

                String carrierDesc = "";
                if(carrier != null)
                    carrierDesc = carrier[1].replaceAll(",","");
                Text outLine = AirlineDataUtils.parseDelaysWritableToText(dw, originAirportDesc, destAirportDesc, carrierDesc);

                context.write(NullWritable.get(), outLine);
            }
        }
    }
    public int run(String[] allArgs) throws Exception{
        Job job = Job.getInstance(getConf());
        job.setJarByClass(MapSideJoinMRJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapSideJoinMapper.class);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile((new File(args[2])).toURI());
        job.addCacheFile((new File(args[3])).toURI());

        job.waitForCompletion(true);
        return 0;
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        ToolRunner.run(new MapSideJoinMRJob(), args);
    }
}
