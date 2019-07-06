package com.ittzg.hadoop.orderv2;

import com.ittzg.hadoop.order.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/6 17:18
 */
public class OrderSortGroupingComparator extends WritableComparator {
    protected OrderSortGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean orderA = (OrderBean) a;
        OrderBean orderB = (OrderBean) b;
        return orderA.getOrderId().compareTo(orderB.getOrderId());
    }
}
