package com.ittzg.storm.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/8/1 15:24
 */
public class WordCountSpout extends BaseRichSpout{
    private static final long serialVersionUID = 2L;
    private BufferedReader br;
    private SpoutOutputCollector collector = null;
    private String str = null;


    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.br = new BufferedReader(new InputStreamReader(new FileInputStream("j:/data/word.log"), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        // 循环调用的方法
        try {
            while ((str = this.br.readLine()) != null) {
                // 发送出去
                collector.emit(new Values(str));
            }
        } catch (Exception e) {
            try {
                br.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 声明输出字段类型
        declarer.declare(new Fields("log"));
    }
}
