package com.ittzg.storm.uv;

import com.ittzg.storm.pv.PvCountBolt;
import com.ittzg.storm.pv.PvSplitBolt;
import com.ittzg.storm.pv.PvSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/8/1 19:40
 */
public class UvMain {
    public static void main(String[] args) throws Exception {
        // 1 创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();

        // 2 设置Spout和bolt
        builder.setSpout("UvSpout", new UvSpout(), 1);
        builder.setBolt("UvSplitBolt", new UvSplitBolt(), 4).shuffleGrouping("UvSpout");
        builder.setBolt("UvCountBolt", new UvCountBolt(), 1).shuffleGrouping("UvSplitBolt");

        // 3 配置Worker开启个数
        Config conf =  new Config();
        conf.setNumWorkers(2);

        // 4 本地模式提交
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("uvTopology", conf, builder.createTopology());
    }
}
