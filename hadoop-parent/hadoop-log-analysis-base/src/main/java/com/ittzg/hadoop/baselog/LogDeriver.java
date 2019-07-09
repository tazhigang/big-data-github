package com.ittzg.hadoop.baselog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/8 23:39
 */
public class LogDeriver {

    static class LogMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(isLegal(value.toString(),context)){
                context.write(value,NullWritable.get());
            }
            return;
        }
        //对日志中的每一行进行数据清洗，去除少于或者等于11个字段的日志
        private boolean isLegal(String line, Context context){
            String[] split = line.split(" ");
            if(split.length>11){
                context.getCounter("log","true").increment(1); //计数器的使用
                return true;
            }
            context.getCounter("log","false").increment(1); //计数器的使用
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/log/input";
        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/log/output";

        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);
        job.setJar("F:\\big-data-github\\hadoop-parent\\hadoop-log-analysis-base\\target\\hadoop-log-analysis-base-1.0-SNAPSHOT.jar");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 3 关联map
        job.setMapperClass(LogMapper.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

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
