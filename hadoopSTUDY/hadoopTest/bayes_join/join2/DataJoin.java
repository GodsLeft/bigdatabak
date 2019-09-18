//package join;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
//import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
//import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
//import DataJoinMapperBase;
//import DataJoinReducerBase;
//import TaggedMapOutput;

public class DataJoin extends Configured implements Tool{
    public static class MapClass extends DataJoinMapperBase {
    	protected Text generateInputTag(String inputFile){
	    String dataSource = inputFile.split("-")[0];
            return new Text(dataSource);
	}

	protected Text generateGroupKey(TaggedMapOutput aRecord) {
	    String line = ((Text) aRecord.getData()).toString();
	    String[] tokens = line.split("\t");
	    String groupKey = tokens[0];
	    return new Text(groupKey);
	}

	protected TaggedMapOutput generateTaggedMapOutput(Object value){
	    TaggedWritable retv = new TaggedWritable((Text) value);
	    retv.setTag(this.inputTag);
	    return retv;
	}
    }

    public static class Reduce extends DataJoinReducerBase {
    	protected TaggedMapOutput combine(Object[] tags, Object[] values){
	    if(tags.length < 2) return null;
	    String joinedStr = "";
	    for(int i=0; i < values.length; i++){
	    	if(i > 0) joinedStr += ",";
		TaggedWritable tw = (TaggedWritable)values[i];
		String line = ((Text) tw.getData()).toString();
		String[] tokens = line.split("\t",2);
		joinedStr += tokens[1];
	    }
	    TaggedWritable retv = new TaggedWritable(new Text(joinedStr));
	    retv.setTag((Text)tags[0]);
	    return retv;
	}
    }

    public static class TaggedWritable extends TaggedMapOutput{
    	private Writable data;
	
	public TaggedWritable(){
	    this.tag = new Text("");
	    this.data = new Text("Test");
	}

	public TaggedWritable(Writable data){
	    this.tag = new Text("");
	    this.data = data;
	}

	public Writable getData(){
	    return data;
	}

	public void write(DataOutput out) throws IOException{
	    this.tag.write(out);
	    this.data.write(out);
	}

	public void readFields(DataInput in) throws IOException{
	    this.tag.readFields(in);
	    this.data.readFields(in);
	}
    }

    public int run(String[] args) throws Exception {
    	Configuration conf = getConf();
	JobConf job = new JobConf(conf, DataJoin.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setJobName("DataJoin");
	job.setMapperClass(MapClass.class);
	job.setReducerClass(Reduce.class);
	job.setInputFormat(TextInputFormat.class);
	job.setOutputFormat(TextOutputFormat.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(TaggedWritable.class);
	job.set("mapred.textoutputformat.separator", ",");

	JobClient.runJob(job);
	return 0;
    }

    public static void main(String[] args) throws Exception {
    	int res = ToolRunner.run(new Configuration(), new DataJoin(), args);
	System.exit(res);
    }
}
