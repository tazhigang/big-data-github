package com.ittzg.mongodb;

import com.alibaba.fastjson.JSON;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/11 23:13
 */
public class MDBClientUtil {
    MongoDatabase db = null;
    MongoCollection<Document> usersDB =null;
    @Before
    public void init(){ //连接mongodb服务器
        MongoClient mongoDBClient = new MongoClient("127.0.0.1", 27017);
        db = mongoDBClient.getDatabase("test");
        usersDB = db.getCollection("users");
    }

    @Test
    public void testInsert(){

        Document doc = new Document("name", "汤姆");
        doc.append("age",18);
        doc.append("shuxin","老鼠");
        usersDB.insertOne(doc);
    }

    /**
     * 添加一个对象
     */
    @Test
    public void testInserObj(){
        User user = new User("杰瑞", 21, "猫");
        String userJsonStr = JSON.toJSONString(user);
        Document doc = Document.parse(userJsonStr);
        usersDB.insertOne(doc);
    }

    /**
     * 查询一个对象
     */
    @Test
    public void testFind(){
        Document doc = usersDB.find().first();
        User user = JSON.parseObject(doc.toJson(), User.class);
        System.out.println(user);
    }

    /**
     * 删除一个文档
     */
    @Test
    public void testDelete(){
        usersDB.deleteOne(Filters.eq("name","杰瑞"));
    }

    /**
     * 更新一个文档
     */
    @Test
    public void testUpdate(){
        FindIterable<Document> documents = usersDB.find();
        for (Document document : documents) {
            System.out.println(document.toJson());
        }
        usersDB.updateOne(Filters.eq("name","杰瑞"), new Document("$set" , new Document("like","play2")) );
        System.out.println("===============");
        FindIterable<Document> documents2 = usersDB.find();
        for (Document document : documents2) {
            System.out.println(document.toJson());
        }
    }
}
