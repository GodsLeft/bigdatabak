package com.zhu.c8;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import org.junit.Test;
import org.junit.Before;
import junit.framework.TestCase;

public class WordCountMRUnitTest extends TestCase{
    private Mapper mapper;
    private Reducer reducer;
    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;
    private MapReduceDriver mapReduceDriver;

    @Before
    public void setUp(){
        mapper = new WordCountMapper();
        mapDriver = new MapDriver(mapper);

        reducer = new WordCountReducer();
        reduceDriver = new ReduceDriver(reducer);

        mapReduceDriver = new MapReduceDriver(mapper, reducer);
    }

    @Test
    public void testWordCountMapper() throws Exception{
        mapDriver
            .withInput(new LongWritable(1), new Text("map1"))
            .withInput(new LongWritable(2), new Text("reduce1"))
            .withOutput(new Text("map1"), new IntWritable(1))
            .withOutput(new Text("reduce1"), new IntWritable(1))
            .runTest();
    }

    @Test
    public void testWordCountReducer() throws Exception{
        Text key1 = new Text("map2");
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));

        Text key2 = new Text("reducer2");
        List<IntWritable> values2 = new ArrayList<IntWritable>();
        values2.add(new IntWritable(1));
        values2.add(new IntWritable(1));
        values2.add(new IntWritable(1));

        reduceDriver
            .withInput(key1, values1)
            .withInput(key2, values2)
            .withOutput(key1, new IntWritable(2))
            .withOutput(key2, new IntWritable(3))
            .runTest();
    }

    @Test
    public void testWordCountMapReducer() throws Exception{
        mapReduceDriver
            .withInput(new LongWritable(1), new Text("map3"))
            .withInput(new LongWritable(2), new Text("map3"))
            .withInput(new LongWritable(3), new Text("reduce3"))
            .withOutput(new Text("map3"), new IntWritable(2))
            .withOutput(new Text("reduce3"), new IntWritable(1))
            .runTest();
    }
}
