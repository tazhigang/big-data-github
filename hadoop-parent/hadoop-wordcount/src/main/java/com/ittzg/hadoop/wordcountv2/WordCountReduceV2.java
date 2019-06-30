package com.ittzg.hadoop.wordcountv2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/6/29 21:13
 * @describe:
 */
public class WordCountReduceV2 extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int count= 0;
        while (iterator.hasNext()){
            count += iterator.next().get();
        }
        outputCollector.collect(text,new IntWritable(count));
    }
}
