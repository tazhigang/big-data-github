package com.ittzg.hadoop.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/1 22:49
 * 在hadoop上的传输数据的bean必须实现Writable的接口
 */
public class FlowBean implements Writable,Comparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    //反序列化时，需要反射调用空参构造函数，所以必须有
    public FlowBean() {
        super();
    }
    //
    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = this.upFlow+this.downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    //序列化方法
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }
    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();

    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    // 如果需要将自定义的bean放在key中传输，则还需要实现comparable接口，因为mapreduce框中的shuffle过程一定会对key进行排序
    public int compareTo(FlowBean flowBean) {
        return this.sumFlow >flowBean .getSumFlow() ? -1 : this.sumFlow==flowBean .getSumFlow()?0:1;
    }
}
