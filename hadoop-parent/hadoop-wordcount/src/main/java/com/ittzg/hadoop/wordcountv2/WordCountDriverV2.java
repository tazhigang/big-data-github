package com.ittzg.hadoop.wordcountv2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.net.URI;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/6/29 21:13
 * @describe:
 */

// 配置程序运行参数 -DHADOOP_USER_NAME=hadoop
    // 解决：Exception in thread "main" org.apache.hadoop.security.AccessControlException: Permission denied: user=tuo, access=EXECUTE, inode="/tmp":hadoop:supergroup:drwx------

/**
 * 将jod提交到远程运行
 */
public class WordCountDriverV2 {
    public static void main(String[] args) throws Exception {
        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/input";
        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/output/v2";
        Configuration configuration = new Configuration();
        configuration.addResource("core-site.xml");
        configuration.addResource("hdfs-site.xml");
        configuration.addResource("mapred-site.xml");
        configuration.addResource("yarn-site.xml");
        // 以下配置是为了解决 org.apache.hadoop.util.Shell$ExitCodeException: /bin/bash: line 0: fg: no job control
        configuration.set("mapreduce.app-submission.cross-platform","true");
        JobConf job = new JobConf(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-ip-101:9000"),configuration,"hadoop");
        // 以下是为了解决 WordCountMapperV2和WordCountReduceV2找不到的异常，需要设置jar在本地的路径
        job.setJar("F:\\big-data-github\\hadoop-parent\\hadoop-wordcount\\target\\hadoop-wordcount-1.0-SNAPSHOT.jar");
        job.setJobName("wordCount");

        // 3. 设置mapper和reduce的class类
        job.setMapperClass(WordCountMapperV2.class);
        job.setReducerClass(WordCountReduceV2.class);
        // 4. 设置mapper数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        Path outPath = new Path(output);
        // 方便程序重复运行
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        JobClient.runJob(job);
        System.exit(0);
    }

}
