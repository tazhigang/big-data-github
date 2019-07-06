package com.ittzg.hadoop.orderproduct;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/6 20:49
 */
public class OrderAndProductBean implements WritableComparable<OrderAndProductBean> {

    private String orderId;
    private String pdId;
    private String pdName;
    private String account;
    private String flag;//判断数据是来自哪个表 "0"表示来自order.txt,"1"表示来自pd.txt

    public OrderAndProductBean() {
    }

    public OrderAndProductBean(String orderId, String pdId, String pdName, String account, String flag) {
        this.orderId = orderId;
        this.pdId = pdId;
        this.pdName = pdName;
        this.account = account;
        this.flag = flag;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPdId() {
        return pdId;
    }

    public void setPdId(String pdId) {
        this.pdId = pdId;
    }

    public String getPdName() {
        return pdName;
    }

    public void setPdName(String pdName) {
        this.pdName = pdName;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return orderId + "\t" + pdName + "\t"+ account ;
    }

    public void write(DataOutput output) throws IOException {
        output.writeUTF(this.orderId);
        output.writeUTF(this.pdId);
        output.writeUTF(this.pdName);
        output.writeUTF(this.account);
        output.writeUTF(this.flag);
    }

    public void readFields(DataInput input) throws IOException {
        this.orderId = input.readUTF();
        this.pdId = input.readUTF();
        this.pdName = input.readUTF();
        this.account = input.readUTF();
        this.flag = input.readUTF();
    }
    public int compareTo(OrderAndProductBean o) {
        return -1;
    }
}
