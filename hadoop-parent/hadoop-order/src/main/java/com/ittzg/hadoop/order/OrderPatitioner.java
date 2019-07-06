package com.ittzg.hadoop.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/6 16:31
 */
public class OrderPatitioner extends Partitioner<OrderBean,NullWritable> {
    volatile int count = -1;
    volatile Map<String,Integer> map= new HashMap<String,Integer>();
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        if(map.containsKey(orderBean.getOrderId())){
            return map.get(orderBean.getOrderId());
        }else{
            count ++;
            map.put(orderBean.getOrderId(),count);
            return count;
        }
    }
}
