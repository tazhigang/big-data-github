package com.ittzg.storm.uv;

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
 * @date: 2019/8/1 19:38
 */
public class UvSplitBolt  extends BaseRichBolt {
    private OutputCollector collector;
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String content = input.getString(0);
        String[] split = content.split("\t");
        if(split!=null && split.length>3){
            if(!StringUtils.isEmpty(split[3])){
                collector.emit(new Values(split[3],1L) );
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip","count"));
    }
}
