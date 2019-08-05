package com.ittzg.storm.pv;

import org.apache.commons.lang.StringUtils;
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
 * @date: 2019/8/1 19:05
 */
public class PvSplitBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Long pv=0L;
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String content = input.getString(0);
        String[] split = content.split("\t");
        if(split!=null && split.length>2){
            if(!StringUtils.isEmpty(split[1])){
                pv++;
            }
        }
        collector.emit(new Values(Thread.currentThread().getId(),pv) );
        System.err.println("threadid:" + Thread.currentThread().getId() + "  pv:" + pv);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("threadId","count"));
    }
}
