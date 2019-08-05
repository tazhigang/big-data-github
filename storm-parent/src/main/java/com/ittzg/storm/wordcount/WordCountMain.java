package com.ittzg.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/8/1 16:02
 */
public class WordCountMain {
    public static void main(String[] args) throws Exception {
        // 1 创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();

        // 2 设置Spout和bolt
        builder.setSpout("WordCountSpout", new WordCountSpout(), 1);
        builder.setBolt("WordSplitBolt", new WordSplitBolt(), 4).shuffleGrouping("WordCountSpout");
        builder.setBolt("WordCountBolt", new WordCountBolt(), 1).shuffleGrouping("WordSplitBolt");

        // 3 配置Worker开启个数
        Config conf =  new Config();
        conf.setNumWorkers(2);

        if (args.length > 0) {
            try {
                // 4 分布式提交
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else {
            // 5 本地模式提交
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("WordCounttopology", conf, builder.createTopology());
        }

    }
}
