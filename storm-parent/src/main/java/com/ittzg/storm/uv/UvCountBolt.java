package com.ittzg.storm.uv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/8/1 19:40
 */
public class UvCountBolt  extends BaseRichBolt {
    private Map<String,Long> map = new HashMap<String,Long>();
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        
    }

    public void execute(Tuple input) {
        String ip = input.getString(0);
        Long count = input.getLong(1);

        if(map.containsKey(ip)){
            map.put(ip,map.get(ip)+count);
        }else{
            map.put(ip,count);
        }
        System.err.println("-======================================\t ip:"+ip+"\t count:"+map.get(ip));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
