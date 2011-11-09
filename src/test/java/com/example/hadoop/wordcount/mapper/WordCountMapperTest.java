package com.example.hadoop.wordcount.mapper;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WordCountMapperTest {

    WordCountMapper mapper;

    WordCountMapper.Context context;

    @Before
    public void setUp() throws Exception {
        mapper = new WordCountMapper();
        context = mock(WordCountMapper.Context.class);
    }

    @After
    public void tearDown() throws Exception {
        mapper = null;
        context = null;
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Text value = new Text("Hello World, Bye World!");
        mapper.map(null, value, context);
        verify(context).write(new Text("Hello"), WordCountMapper.ONE);
        verify(context).write(new Text("Bye"), WordCountMapper.ONE);
        verify(context, times(2)).write(new Text("World"), WordCountMapper.ONE);
    }

}
