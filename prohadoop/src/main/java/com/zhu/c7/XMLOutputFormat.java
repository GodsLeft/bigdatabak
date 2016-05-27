package com.zhu.c7;
import com.zhu.utils.*;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.*;

public class XMLOutputFormat extends FileOutputFormat<LongWritable, Text>{

    protected static class XMLRecordWriter extends RecordWriter<LongWritable, Text>{
        private DataOutputStream out;
        public XMLRecordWriter(DataOutputStream out) throws IOException{
            this.out = out;
            out.writeBytes("<recs>\n");
        }

        private void writeTag(String tag, String value) throws IOException{
            out.writeBytes("<" + tag + ">" + value + "</" + tag + ">");
        }

        public synchronized void write(LongWritable key, Text value) throws IOException{
            out.writeBytes("<rec>");
            this.writeTag("key", Long.toString(key.get()));
            String[] contents = value.toString().split(",");
            String year = AirlineDataUtils.getYear(contents);
            this.writeTag("year", year);
            out.writeBytes("</recs>\n");
        }
        public synchronized void close(TaskAttemptContext job) throws IOException{
            try{
                out.writeBytes("</recs>\n");
            }finally{
                out.close();
            }
        }
    }
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException{
        String extension = ".xml";
        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new XMLRecordWriter(fileOut);
    }

}
