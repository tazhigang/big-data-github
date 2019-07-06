package com.ittzg.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;


/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/6/23 1:58
 * @describe:
 */
public class HDFSClient {
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
     * 获取文件系统
     */
    @Test
    public void getFileSystem(){
        System.out.println(fileSystem.toString());
    }

    /**
     * 上传文件
     */
    @Test
    public void putFileToHDFS(){
        try {
            fileSystem.copyFromLocalFile(new Path("J:\\word.txt"),new Path("/user/hadoop/input/word.txt"));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    /**
     * 下载文件
     */
    @Test
    public void getFileFromHDFS(){
        try {
            fileSystem.copyToLocalFile(new Path("/user/hadoop/shaoniangexing.txt"),new Path("J:\\少年歌行.txt"));
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
            boolean mkdirs = fileSystem.mkdirs(new Path("/user/hadoop/input"));
            System.out.println(mkdirs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 修改hdfs上文件的名称
     */
    @Test
    public void renameFileAtHDFS(){
        try {
            fileSystem.rename(new Path("/user/hadoop/hello.txt"),new Path("/user/hadoop/少年歌行.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 查看hdfs上的文件列表
     */
    @Test
    public void readFileListAtHDFS(){
        try {
            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"),true);
            while (locatedFileStatusRemoteIterator.hasNext()){
                System.out.println(locatedFileStatusRemoteIterator.next().toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void rmFile() throws IOException {
        fileSystem.delete(new Path("/user/hadoop/order/input/order.txt"),true);
    }
    @Test
    public void testDownload() throws IOException {
        URL url = new URL("https://oscimg.oschina.net/oscnet/3ee44cb5de42b5fa57079de388b2ce9e911.jpg");
        URLConnection conn = url.openConnection();
        conn.setConnectTimeout(5*1000);
        InputStream inputStream = conn.getInputStream();
        byte[] bs = new byte[1024];
        int len;
        File file = new File("J://a.jpg");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        while((len=inputStream.read(bs))!=-1){
            fileOutputStream.write(bs,0,len);

        }
        fileOutputStream.close();
        inputStream.close();
    }
}
