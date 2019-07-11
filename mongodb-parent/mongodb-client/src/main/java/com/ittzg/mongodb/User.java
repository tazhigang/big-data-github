package com.ittzg.mongodb;

import org.bson.types.ObjectId;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/11 23:26
 */
public class User {

    private ObjectId _id;

    private String name;
    private Integer age;
    private String shuxin;

    public User(String name, Integer age, String shuxin) {
        this.name = name;
        this.age = age;
        this.shuxin = shuxin;
    }

    public User() {
    }

    public ObjectId get_id() {
        return _id;
    }

    public void set_id(ObjectId _id) {
        this._id = _id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getShuxin() {
        return shuxin;
    }

    public void setShuxin(String shuxin) {
        this.shuxin = shuxin;
    }

    @Override
    public String toString() {
        return "User{" +
                "_id=" + _id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", shuxin='" + shuxin + '\'' +
                '}';
    }
}
