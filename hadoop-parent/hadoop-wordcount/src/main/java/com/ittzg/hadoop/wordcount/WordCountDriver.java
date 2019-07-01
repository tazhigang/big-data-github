package com.ittzg.hadoop.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/6/29 21:13
 * @describe:
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
//        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/input/";
//        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/output";
        // 1. 获取job的对象信息
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFs","hadoop-ip-101:9000");
        JobConf job = new JobConf(conf);
        // 2. 设置加载jar的位置
        job.setJarByClass(WordCountDriver.class);
        // 3. 设置mapper和reduce的class类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        // 4. 设置mapper数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5. 设置最终输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6. 设置输入数据和输出数据的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 7.提交job
        JobClient.runJob(job);
        System.exit(0);
    }

}
