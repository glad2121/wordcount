package com.example.hadoop.wordcount;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.hadoop.wordcount.mapper.WordCountMapper;
import com.example.hadoop.wordcount.reducer.WordCountReducer;

/**
 * 単語の出現頻度を数える Hadoop アプリケーション。
 * <p>
 * オリジナルからの変更点：
 * <ul>
 *   <li>Mapper と Reduce を別ファイルに分割し、単体テストを追加した。</li>
 *   <li>入力パスを再帰的に検索するように変更した。</li>
 *   <li>出力パスを実行するごとに生成するように変更した。</li>
 *   <li>単語から句読文字を取り除いた。</li>
 *   <li>ログの出力を追加した。</li>
 * </ul>
 * </p>
 * 
 * @author ITO Yoshiichi
 * @see <a href="http://wiki.apache.org/hadoop/WordCount">WordCount Example</a>
 */
public class WordCount {

    static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    /**
     * アプリケーションを起動します。
     */
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
        //FileInputFormat.addInputPath(job, new Path(inputPath));
        addInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 入力パスを再帰的に追加します。
     */
    static void addInputPaths(Job job, Path path) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.getFileStatus(path).isDir()) {
            for (FileStatus child : fs.listStatus(path)) {
                addInputPaths(job, child.getPath());
            }
        } else {
            FileInputFormat.addInputPath(job, path);
            if (logger.isDebugEnabled()) {
                logger.debug("addInputPath: {}", path);
            }
        }
    }

}
