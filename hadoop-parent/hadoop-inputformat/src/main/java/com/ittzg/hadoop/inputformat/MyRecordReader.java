package com.ittzg.hadoop.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/7 21:44
 */
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit;
    private Configuration configuration;
    
    private BytesWritable value = new BytesWritable();
    private boolean isProcess = false; //表示是否读取完成文件内容

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!isProcess){
            // 获取文件在Hdfs上的路径,并读取文件内容
            FileSystem fileSystem = null;
            FSDataInputStream inputStream = null;
            try {
                Path path = fileSplit.getPath();
                fileSystem = path.getFileSystem(configuration);
                inputStream = fileSystem.open(path);
                byte[] buf = new byte[(int) fileSplit.getLength()];
                IOUtils.readFully(inputStream,buf,0,buf.length);
                value.set(buf,0,buf.length);
            } finally {
                IOUtils.closeStream(fileSystem);
            }
            isProcess = true;
            return true;
        }
        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
