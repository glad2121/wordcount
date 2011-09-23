package com.example.hadoop.wordcount.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    static final Logger logger =
            LoggerFactory.getLogger(WordCountReducer.class);

    @Override
    protected void reduce(
            Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        List<IntWritable> list = new ArrayList<IntWritable>();
        int sum = 0;
        for (IntWritable value : values) {
            list.add(value);
            sum += value.get();
        }
        logger.debug("input: ({}, {})", key, list);
        logger.debug("output: ({}, {})", key, sum);
        context.write(key, new IntWritable(sum));
    }

}
