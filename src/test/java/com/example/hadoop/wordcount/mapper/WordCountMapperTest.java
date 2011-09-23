package com.example.hadoop.wordcount.mapper;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WordCountMapperTest {

    WordCountMapper mapper;

    WordCountMapper.Context context;

    RecordWriter<Text, IntWritable> output;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        output = mock(RecordWriter.class);
        mapper = new WordCountMapper() {{
           context = new Context(
                   new Configuration(), new TaskAttemptID(),
                   null, output, null, null, null); 
        }};
    }

    @After
    public void tearDown() throws Exception {
        mapper = null;
        context = null;
        output = null;
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Text value = new Text("Hello World, Bye World!");
        mapper.map(null, value, context);
        verify(output).write(new Text("Hello"), WordCountMapper.ONE);
        verify(output).write(new Text("Bye"), WordCountMapper.ONE);
        verify(output, times(2)).write(new Text("World"), WordCountMapper.ONE);
    }

}
