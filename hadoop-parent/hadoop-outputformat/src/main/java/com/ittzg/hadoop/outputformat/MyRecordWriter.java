package com.ittzg.hadoop.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/7 22:57
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream baiduOut = null;
    private FSDataOutputStream otherOut = null;


    public MyRecordWriter(TaskAttemptContext job) {
        Configuration configuration = job.getConfiguration();
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            //创建两个输入流
            Path baiduPath = new Path("j:/url/baidu_url.txt");
            Path otherPath = new Path("j:/url/other_url.txt");
            baiduOut = fileSystem.create(baiduPath);
            otherOut = fileSystem.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if(key.toString().contains("baidu")){
            baiduOut.write(key.toString().getBytes());
        }else{
            otherOut.write(key.toString().getBytes());
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if(baiduOut != null){
            baiduOut.close();
        }
        if(otherOut != null){
            otherOut.close();
        }
    }
}
