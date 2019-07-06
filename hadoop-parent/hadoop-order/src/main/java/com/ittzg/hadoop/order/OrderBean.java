package com.ittzg.hadoop.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/6 16:34
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private String proName;
    private Double price;

    public OrderBean() {
    }

    public OrderBean(String orderId, String proName, Double price) {
        this.orderId = orderId;
        this.proName = proName;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProName() {
        return proName;
    }

    public void setProName(String proName) {
        this.proName = proName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" +proName + "\t" +price;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.proName);
        out.writeDouble(this.price);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.proName = in.readUTF();
        this.price = in.readDouble();
    }

    public int compareTo(OrderBean orderBean) {
        int result = orderBean.orderId.compareTo(this.orderId);
        if(result == 0){
            result = orderBean.price.compareTo(this.price);
        }
        return result;
    }
}
