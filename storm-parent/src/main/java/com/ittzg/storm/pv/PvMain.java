package com.ittzg.storm.pv;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/8/1 19:25
 */
public class PvMain {
    public static void main(String[] args) throws Exception {
        // 1 创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();

        // 2 设置Spout和bolt
        builder.setSpout("PvSpout", new PvSpout(), 1);
        builder.setBolt("PvSplitBolt", new PvSplitBolt(), 4).shuffleGrouping("PvSpout");
        builder.setBolt("PvCountBolt", new PvCountBolt(), 1).shuffleGrouping("PvSplitBolt");

        // 3 配置Worker开启个数
        Config conf =  new Config();
        conf.setNumWorkers(2);

        // 4 本地模式提交
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("pvTopology", conf, builder.createTopology());
    }
}
