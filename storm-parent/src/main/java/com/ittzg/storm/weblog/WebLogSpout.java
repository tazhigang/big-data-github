package com.ittzg.storm.weblog;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class WebLogSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;

	private BufferedReader br;
	private SpoutOutputCollector collector = null;
	private String str = null;


	public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector collector) {
		// 打开输入的文件
		try {
			this.collector = collector;
			this.br = new BufferedReader(new InputStreamReader(new FileInputStream("j:/data/website.log"), "UTF-8"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {

	}

	public void activate() {

	}

	public void deactivate() {

	}

	public void nextTuple() {
		// 循环调用的方法
		try {
			while ((str = this.br.readLine()) != null) {
				// 发送出去
				collector.emit(new Values(str));
			}
		} catch (Exception e) {
		}
	}

	public void ack(Object o) {

	}

	public void fail(Object o) {

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// 声明输出字段类型
		outputFieldsDeclarer.declare(new Fields("log"));

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
