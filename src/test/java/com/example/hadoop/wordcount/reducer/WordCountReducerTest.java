package com.example.hadoop.wordcount.reducer;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WordCountReducerTest {

    WordCountReducer reducer;

    WordCountReducer.Context context;

    RawKeyValueIterator input;

    RecordWriter<Text, IntWritable> output;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        input = mock(RawKeyValueIterator.class);
        output = mock(RecordWriter.class);
        reducer = new WordCountReducer() {{
            context = new Context(new Configuration(), new TaskAttemptID(),
                    input, null, null, output, null, null, null,
                    Text.class, IntWritable.class);
        }};
    }

    @After
    public void tearDown() throws Exception {
        reducer = null;
        context = null;
        input = null;
        output = null;
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Text key = new Text("Hoge");
        List<IntWritable> values = Arrays.asList(
                new IntWritable(1), new IntWritable(2), new IntWritable(3));
        reducer.reduce(key, values, context);
        verify(output).write(key, new IntWritable(6));
    }

}
