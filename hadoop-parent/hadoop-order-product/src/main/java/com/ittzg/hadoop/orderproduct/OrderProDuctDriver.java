package com.ittzg.hadoop.orderproduct;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/6 20:46
 */
public class OrderProDuctDriver {
    public static class OrderProDuctMapper extends Mapper<LongWritable,Text,Text,OrderAndProductBean>{
        OrderAndProductBean orderAndProductBean= new OrderAndProductBean();
        Text text = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName(); //获取切片名称
            String[] split = line.split("\t");
            if(fileName.contains("order")){
                orderAndProductBean.setOrderId(split[0]);
                orderAndProductBean.setPdId(split[1]);
                orderAndProductBean.setAccount(split[2]);
                orderAndProductBean.setPdName("");
                orderAndProductBean.setFlag("0");
                text.set(split[1]);
            }else{
                orderAndProductBean.setOrderId(""); //必须设置为空串，否则或报空指针异常
                orderAndProductBean.setAccount("");
                orderAndProductBean.setPdId(split[0]);
                orderAndProductBean.setPdName(split[1]);
                orderAndProductBean.setFlag("1");
                text.set(split[0]);
            }
            context.write(text,orderAndProductBean);
        }
    }
    public static class OrderProDuctReduce extends Reducer<Text,OrderAndProductBean,OrderAndProductBean,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<OrderAndProductBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<OrderAndProductBean>  orderAndProductBeanslist= new ArrayList<OrderAndProductBean>();
            OrderAndProductBean pdBean = new OrderAndProductBean();
            System.out.println(key);
            for (OrderAndProductBean value : values) {
                if("1".equals(value.getFlag())){
                    try {
                        BeanUtils.copyProperties(pdBean,value);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    OrderAndProductBean orderbean = new OrderAndProductBean();
                    try {
                        BeanUtils.copyProperties(orderbean,value);
                        orderAndProductBeanslist.add(orderbean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }
            for (OrderAndProductBean orderAndProductBean : orderAndProductBeanslist) {
                System.out.println(orderAndProductBean);
            }
            // 拼接bean将bean写出
            for (OrderAndProductBean orderAndProductBean : orderAndProductBeanslist) {
                orderAndProductBean.setPdName(pdBean.getPdName());
                context.write(orderAndProductBean,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception{
        // 设置输入输出路径
        String input = "hdfs://hadoop-ip-101:9000/user/hadoop/order_product/input";
        String output = "hdfs://hadoop-ip-101:9000/user/hadoop/order_product/output";
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);
        //
        job.setJar("F:\\big-data-github\\hadoop-parent\\hadoop-order-product\\target\\hadoop-order-product-1.0-SNAPSHOT.jar");

        job.setMapperClass(OrderProDuctMapper.class);
        job.setReducerClass(OrderProDuctReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderAndProductBean.class);

        job.setOutputKeyClass(OrderAndProductBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-ip-101:9000"),conf,"hadoop");
        Path outPath = new Path(output);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job,outPath);

        boolean bool = job.waitForCompletion(true);
        System.exit(bool?0:1);
    }
}
