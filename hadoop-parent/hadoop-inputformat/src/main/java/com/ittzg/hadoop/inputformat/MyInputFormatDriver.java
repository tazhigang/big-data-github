package com.ittzg.hadoop.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/7 21:45
 */
public class MyInputFormatDriver {
    static class MyMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{
        Text filePath = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            Path path = inputSplit.getPath();
            filePath.set(path.toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            System.out.println(value);
           context.write(filePath,value);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/inputformat/input";
        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/inputformat/output";

        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyInputFormatDriver.class);

        job.setInputFormatClass(MyFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(MyMapper.class);

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
