package com.ittzg.hadoop.flowv2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/6 14:19
 */
public class FlowPartition extends Partitioner<Text,FlowBean> {
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        // 以电话号码前三位分区
        String phoneNum = text.toString().substring(0,3);
        int partitons = 3; //共分为4个区
        if("135".equals(phoneNum)){
            partitons = 0;
        }else if("136".equals(phoneNum)){
            partitons = 1;
        }else if("137".equals(phoneNum)){
            partitons = 2;
        }
        return partitons;
    }
}
