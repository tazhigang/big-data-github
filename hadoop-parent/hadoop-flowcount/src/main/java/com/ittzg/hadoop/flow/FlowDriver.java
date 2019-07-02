package com.ittzg.hadoop.flow;

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
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/1 22:49
 * @describe:
 */
public class FlowDriver {
    public static class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取一行数据
            String line = value.toString();
            // 截取字段
            String[] split = line.split("\t");
            long upFlow = Long.parseLong(split[split.length-3]);
            long downFlow = Long.parseLong(split[split.length-2]);
            // 构造FlowBean
            FlowBean flowBean = new FlowBean(upFlow, downFlow);
            context.write(new Text(split[1]),flowBean);
        }
    }

    public static class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upFlow = 0;
            long downFlow =  0;
            for (FlowBean flowBean : values) {
                upFlow += flowBean.getUpFlow();
                downFlow +=flowBean.getDownFlow();
            }

            FlowBean flowBean = new FlowBean(upFlow, downFlow);
            context.write(new Text(key),flowBean);
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        // 设置输入输出路径
        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/flow/input";
        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/flow/output";
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);

        job.setJar("/big-data-github/hadoop-parent/hadoop-flowcount/target/hadoop-flowcount-1.0-SNAPSHOT.jar");

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-ip-101:9000"),conf,"hadoop");
        Path outPath = new Path(output);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job,outPath);

        job.submit();

        boolean bool = job.waitForCompletion(true);
        System.exit(bool?0:1);
    }

}
