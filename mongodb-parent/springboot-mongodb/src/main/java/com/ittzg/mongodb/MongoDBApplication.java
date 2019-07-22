package com.ittzg.mongodb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/22 13:43
 * 由于只做演示故省略业务层
 */
@SpringBootApplication
public class MongoDBApplication {
    public static void main(String[] args) {
        SpringApplication.run(MongoDBApplication.class,args);
    }
}
