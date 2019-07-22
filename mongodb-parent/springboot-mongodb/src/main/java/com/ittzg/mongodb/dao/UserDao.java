package com.ittzg.mongodb.dao;

import com.ittzg.mongodb.entity.UserEntity;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/22 14:32
 */
public interface UserDao {
    // 添加或者更新数据
    boolean saveOrUpdate(UserEntity userEntity);
    // 查找返回实体
    List<UserEntity> findByUserName(String username);
    // 删除
    boolean deleteByUserName(String username);
}
