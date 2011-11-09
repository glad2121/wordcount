package com.example.hadoop.wordcount.reducer;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WordCountReducerTest {

    WordCountReducer reducer;

    WordCountReducer.Context context;

    @Before
    public void setUp() throws Exception {
        reducer = new WordCountReducer();
        context = mock(WordCountReducer.Context.class);
    }

    @After
    public void tearDown() throws Exception {
        reducer = null;
        context = null;
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Text key = new Text("Hoge");
        List<IntWritable> values = Arrays.asList(
                new IntWritable(1), new IntWritable(2), new IntWritable(3));
        reducer.reduce(key, values, context);
        verify(context).write(key, new IntWritable(6));
    }

}
