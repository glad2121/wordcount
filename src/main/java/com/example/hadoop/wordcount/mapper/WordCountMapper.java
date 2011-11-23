package com.example.hadoop.wordcount.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 行を単語に分解する Mapper。
 * 
 * @author ITO Yoshiichi
 */
public class WordCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    static final IntWritable ONE = new IntWritable(1);

    static final Logger logger =
            LoggerFactory.getLogger(WordCountMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        logger.debug("input: ({}, {})", key, value);
        String line = value.toString();
        String[] words = line.split("[\\s\\p{Punct}]+");
        for (String word : words) {
            logger.debug("output: ({}, {})", word, ONE);
            context.write(new Text(word), ONE);
        }
    }

}
