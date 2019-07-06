package com.ittzg.hadoop.orderproduct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/6 20:44
 */
public class HDFSUtil {
    Configuration configuration = new Configuration();
    FileSystem fileSystem = null;

    /**
     * 每次执行添加有@Test注解的方法之前调用
     */
    @Before
    public void init(){
        configuration.set("fs.defaultFs","hadoop-ip-101:9000");
        try {
            fileSystem = FileSystem.get(new URI("hdfs://hadoop-ip-101:9000"),configuration,"hadoop");
        } catch (Exception e) {
            throw new RuntimeException("获取hdfs客户端连接异常");
        }
    }
    /**
     * 每次执行添加有@Test注解的方法之后调用
     */
    @After
    public void closeRes(){
        if(fileSystem!=null){
            try {
                fileSystem.close();
            } catch (IOException e) {
                throw new RuntimeException("关闭hdfs客户端连接异常");
            }
        }
    }
    /**
     * 上传文件
     */
    @Test
    public void putFileToHDFS(){
        try {
//            fileSystem.copyFromLocalFile(new Path("F:\\big-data-github\\hadoop-parent\\hadoop-order-product\\src\\main\\resources\\file\\order.txt"),new Path("/user/hadoop/order_product/input/order.txt"));
            fileSystem.copyFromLocalFile(new Path("F:\\big-data-github\\hadoop-parent\\hadoop-order-product\\src\\main\\resources\\file\\pd.txt"),new Path("/user/hadoop/order_product/input/pd.txt"));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
    /**
     * 创建hdfs的目录
     * 支持多级目录
     */
    @Test
    public void mkdirAtHDFS(){
        try {
            boolean mkdirs = fileSystem.mkdirs(new Path("/user/hadoop/order_product/input/order.txt"));
            System.out.println(mkdirs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void rmFile() throws IOException {
        fileSystem.delete(new Path("/user/hadoop/order_product/input/pd.txt"),true);
    }
}
