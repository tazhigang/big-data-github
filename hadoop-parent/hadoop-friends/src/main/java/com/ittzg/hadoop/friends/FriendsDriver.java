package com.ittzg.hadoop.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.Arrays;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/9 22:24
 */
public class FriendsDriver {
    static class FriendMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text textKey = new Text();
        Text textValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] persons = line.split(":");

            String[] friends = persons[1].split(",");
            for (String friend : friends) {
                int i = persons[0].compareTo(friend);
                if(i>0){
                    textKey.set(friend+"-"+persons[0]);
                }else{
                    textKey.set(persons[0]+"-"+friend);
                }
                textValue.set("ok");
                context.write(textKey,textValue);
            }
            Arrays.sort(friends);

            for (int i = 0; i < friends.length - 1; i++) {
                for (int j = i + 1; j < friends.length; j++) {
                    textKey.set(friends[i] + "-" + friends[j]);
                    textValue.set(persons[0]);
                    context.write(textKey,textValue);
                }
            }
        }
    }
    static class FriendCombine extends Reducer<Text,Text,Text,Text>{

    }
    static class FriendReduce extends Reducer<Text,Text,Text,Text>{
        Text textValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder("【");
            for (Text value : values) {
                stringBuilder.append(value+"\t");
            }
            stringBuilder.append("】");
            if(!stringBuilder.toString().contains("ok") ||"【】".equals(stringBuilder.toString().replaceAll("\tok","").replace("ok\t",""))){
                return;
            }
            textValue.set(stringBuilder.toString().replaceAll("\tok","").replace("ok\t",""));
            System.out.println("textKey:"+key.toString()+"\ttextValue:"+textValue.toString());
            context.write(key,textValue);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/friend/input";
        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/friend/output";

        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);
        job.setJar("F:\\big-data-github\\hadoop-parent\\hadoop-friends\\target\\hadoop-friends-1.0-SNAPSHOT.jar");


        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReduce.class);

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
