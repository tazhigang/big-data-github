package com.ittzg.mongodb.dao.impl;

import com.ittzg.mongodb.dao.UserDao;
import com.ittzg.mongodb.entity.UserEntity;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/22 14:32
 */
@Component
public class UserDaoImpl implements UserDao{
    @Autowired
    private MongoOperations mongoOperations;

    @Override
    public boolean saveOrUpdate(UserEntity userEntity) {
        try {
            mongoOperations.save(userEntity);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public List<UserEntity> findByUserName(String username) {
        try {
            Query query = new Query(where("username").is(username));
            List<UserEntity> userEntities = mongoOperations.find(query, UserEntity.class);
            return userEntities;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean deleteByUserName(String username) {
        try {
            Query query = new Query(where("username").is(username));
            mongoOperations.findAllAndRemove(query, UserEntity.class);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
