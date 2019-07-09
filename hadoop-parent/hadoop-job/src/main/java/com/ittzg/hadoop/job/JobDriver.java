package com.ittzg.hadoop.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/9 20:56
 */
public class JobDriver {
    static class JobMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text wordKey = new Text();
        Text wordValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            for (String word : split) {
                wordKey.set(word);
                wordValue.set(fileName+":"+1);
                context.write(wordKey,wordValue);
            }
        }
    }
    static class JobReduce extends Reducer<Text,Text,Text,Text>{
        Map<String,Integer> map = new HashMap();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            map.clear();
            for (Text value : values) {
                String[] split = value.toString().split(":");
                if(map.containsKey(split[0])){
                    map.put(split[0],map.get(split[0])+1);
                }else{
                    map.put(split[0].toString(),1);
                }
            }

            StringBuilder stringBuilder = new StringBuilder("\t");
            Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
            for (Map.Entry<String, Integer> entry : entrySet) {
                stringBuilder.append("\t["+entry.getKey()+"--->"+entry.getValue()+"]");
            }
            context.write(key,new Text(stringBuilder.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/job/input";
        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/job/output";

        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);
        job.setJar("F:\\big-data-github\\hadoop-parent\\hadoop-job\\target\\hadoop-job-1.0-SNAPSHOT.jar");


        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-ip-101:9000"),conf,"hadoop");
        Path outPath = new Path(output);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, outPath);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
