package com.ittzg.hadoop.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * @date: 2019/7/6 16:31
 */
public class OrderDrive {
    public static class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            System.out.println(line);
            String[] split = line.split("\t");
            OrderBean orderBean = new OrderBean(split[0], split[1], Double.parseDouble(split[2]));
            context.write(orderBean,NullWritable.get());
        }
    }

    public static class OrderReduce extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable> {
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception{
        // 设置输入输出路径
        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/order/input";
        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/order/output";
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);
        //
        job.setJar("F:\\big-data-github\\hadoop-parent\\hadoop-order\\target\\hadoop-order-1.0-SNAPSHOT.jar");

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReduce.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-ip-101:9000"),conf,"hadoop");
        Path outPath = new Path(output);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        // 设置分区
        job.setPartitionerClass(OrderPatitioner.class);
        // 设置reduceTask个数
        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job,outPath);

        boolean bool = job.waitForCompletion(true);
        System.exit(bool?0:1);
    }
}
