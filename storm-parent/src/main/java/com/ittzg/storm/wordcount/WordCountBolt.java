package com.ittzg.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/8/1 15:24
 */
public class WordCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = 2L;
    private Map<String, Integer> map = new HashMap<String, Integer>();

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    }

    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer count = input.getInteger(1);

        if(map.containsKey(word)){
            map.put(word,map.get(word)+count);
        }else{
            map.put(word,count);
        }
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.err.println("-======================================"+Thread.currentThread().getId()+"\t word:"+word+"\t count:"+map.get(word));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
