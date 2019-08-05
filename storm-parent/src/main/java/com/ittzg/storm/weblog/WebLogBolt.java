package com.ittzg.storm.weblog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class WebLogBolt implements IRichBolt {
	private Logger logger = LoggerFactory.getLogger(getClass());
	private static final long serialVersionUID = 1L;
    private OutputCollector collector = null;
	private int num = 0;
	private String valueString = null;


	public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	public void execute(Tuple tuple) {
		try {
			// 1 获取传递过来的数据
			valueString = tuple.getStringByField("log");

			// 2 如果输入的数据不为空，行数++
			if (valueString != null) {
				num++;
				System.err.println(Thread.currentThread().getName() + "lines  :" + num + "   session_id:" + valueString.split("\t")[1]);
			}

			// 3 应答Spout接收成功
			collector.ack(tuple);
		} catch (Exception e) {
			// 4 应答Spout接收失败
			collector.fail(tuple);
			e.printStackTrace();
		}
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// 声明输出字段类型
		outputFieldsDeclarer.declare(new Fields(""));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
