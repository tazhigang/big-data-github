package com.ittzg.storm.pv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/8/1 19:06
 */
public class PvCountBolt extends BaseRichBolt {
    private Map<Long,Long> map = new HashMap<Long,Long>();
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            
    }

    public void execute(Tuple input) {
        Long threadId = input.getLong(0);
        Long count = input.getLong(1);
        map.put(threadId,count);

        Long numCount = 0L;
        Iterator<Long> iterator = map.values().iterator();
        while (iterator.hasNext()){
            numCount += iterator.next();
        }
        System.err.println("pv_all:" + numCount);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
