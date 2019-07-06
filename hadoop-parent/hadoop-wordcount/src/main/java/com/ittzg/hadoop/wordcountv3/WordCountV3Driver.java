package com.ittzg.hadoop.wordcountv3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/6/29 21:13
 * @describe:
 */
public class WordCountV3Driver {
    public static class WordCountV3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            for (String word : line.split(" ")) {
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }
    public static class WordCountV3Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count= 0;
            for (IntWritable value : values) {
                count +=value.get();
            }
            context.write(new Text(key),new IntWritable(count));
        }
    }
    public static void main(String[] args) throws Exception {
        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/input";
        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/output/v4";
        Configuration configuration = new Configuration();
        configuration.addResource("core-site.xml");
        configuration.addResource("hdfs-site.xml");
        configuration.addResource("mapred-site.xml");
        configuration.addResource("yarn-site.xml");
        // 以下配置是为了解决 org.apache.hadoop.util.Shell$ExitCodeException: /bin/bash: line 0: fg: no job control
        configuration.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-ip-101:9000"),configuration,"hadoop");
        // 以下是为了解决 WordCountMapperV2和WordCountReduceV2找不到的异常，需要设置jar在本地的路径
        job.setJar("/big-data-github/hadoop-parent/hadoop-wordcount/target/hadoop-wordcount-1.0-SNAPSHOT.jar");
        job.setJobName("wordCount");

        // 3. 设置mapper和reduce的class类
        job.setMapperClass(WordCountV3Mapper.class);
        job.setReducerClass(WordCountV3Reduce.class);
        // 4. 设置mapper数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path outPath = new Path(output);
        // 方便程序重复运行
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        // 5. 设置最终输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        // 处理小文件切片
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
//        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m
        // 6. 设置输入数据和输出数据的路径
        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job,outPath);
        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
