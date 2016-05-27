import java.io.IOException;
import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
public class PercWord extends Configured implements Tool{
    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	String[] tokens = null;
	int hamCount = 7827027;
	int spamCount = 12332367;
	int wordspam = 0;
	int wordham = 0;
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
	    tokens = value.toString().split(",");
	    double percentHam = Integer.parseInt(tokens[1]) * 1000000 / hamCount;
	    double percentSpam = Integer.parseInt(tokens[2]) * 1000000 / spamCount;
	    String lin =  percentHam + "," + percentSpam;
	    //output.collect(new Text("spam"), new IntWritable(Integer.parseInt(tokens[2])));
	    output.collect(new Text(tokens[0]), new Text(lin));
	}
    }
/*
    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text>{
	@Override
    	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
	   // int sum = 0;
	   // while(values.hasNext()){
	   // 	sum += values.next().get();
	   // }
	   // output.collect(key, new );
	}
    }
*/
    public int run(String[] args) throws Exception{
    	Configuration conf = getConf();
	JobConf job = new JobConf(conf, PercWord.class);
	Path in = new Path(args[0]);
	Path out = new Path(args[1]);
	FileInputFormat.setInputPaths(job, in);
	FileOutputFormat.setOutputPath(job, out);

	job.setJobName("PercWord");
	job.setMapperClass(MapClass.class);
	//job.setCombinerClass(Reduce.class);
	//job.setReducerClass(Reduce.class);
	job.setReducerClass(IdentityReducer.class);

	job.setInputFormat(TextInputFormat.class);
	job.setOutputFormat(TextOutputFormat.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	JobClient.runJob(job);
	return 0;
    }

    public static void main(String[] args) throws Exception {
    	int res = ToolRunner.run(new Configuration(), new PercWord(), args);
	System.exit(res);
    }
}
