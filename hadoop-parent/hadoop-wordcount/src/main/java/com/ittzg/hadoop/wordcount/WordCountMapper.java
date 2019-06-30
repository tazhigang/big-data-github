package com.ittzg.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/6/29 21:13
 * @describe:
 */
public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = text.toString();
        for (String word : line.split(" ")) {
            output.collect(new Text(word),new IntWritable(1));
        }
    }
}
