package com.ittzg.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/8/1 15:53
 */
public class WordSplitBolt extends BaseRichBolt {
    private static final long serialVersionUID = 2L;
    private OutputCollector collector;

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String words = input.getString(0);
        String[] split = words.split("\t");
        for (String word : split) {
            collector.emit(new Values(word,1));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "num"));
    }
}
