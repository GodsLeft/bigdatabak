import java.io.IOException;
import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool{
    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	Pattern p = Pattern.compile("\\w+");
	String token = null;

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
	    String line = value.toString().toLowerCase();
	    Matcher m = p.matcher(line);
	    while(m.find()){
		token = m.group();
		if(token.length() > 20) continue;
	    	word.set(token);
		output.collect(word, one);
	    }
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
    	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
	    int sum = 0;
	    while(values.hasNext()){
	    	sum += values.next().get();
	    }
	    if(sum > 20)
	   	 output.collect(key, new IntWritable(sum));
	}
    }

    public int run(String[] args) throws Exception{
    	Configuration conf = getConf();
	JobConf job = new JobConf(conf, WordCount.class);
	Path in = new Path(args[0]);
	Path out = new Path(args[1]);
	FileInputFormat.setInputPaths(job, in);
	FileOutputFormat.setOutputPath(job, out);

	job.setJobName("WordCount");
	job.setMapperClass(MapClass.class);
	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);

	job.setInputFormat(TextInputFormat.class);
	job.setOutputFormat(TextOutputFormat.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	JobClient.runJob(job);
	return 0;
    }

    public static void main(String[] args) throws Exception {
    	int res = ToolRunner.run(new Configuration(), new WordCount(), args);
	System.exit(res);
    }
}
