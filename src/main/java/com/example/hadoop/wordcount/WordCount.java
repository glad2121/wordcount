package com.example.hadoop.wordcount;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.example.hadoop.wordcount.mapper.WordCountMapper;
import com.example.hadoop.wordcount.reducer.WordCountReducer;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        String inputPath  = (args.length > 0) ? args[0] : "input";
        String outputPath = (args.length > 1) ? args[2] : "output/"
                + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
