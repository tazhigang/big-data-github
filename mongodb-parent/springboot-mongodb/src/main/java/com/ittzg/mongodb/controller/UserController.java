package com.ittzg.mongodb.controller;

import com.ittzg.mongodb.dao.UserDao;
import com.ittzg.mongodb.entity.UserEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/22 15:38
 */
@RestController
@RequestMapping(value = "user")
public class UserController {
    @Autowired
    private UserDao userDao;

    @RequestMapping(value = "addOrModify",method = RequestMethod.POST)
    public String addUser(@RequestBody UserEntity userEntity){
        if(userDao.saveOrUpdate(userEntity)){
            return "success";
        }
        return "failure";
    }

    @RequestMapping(value = "deleteByUserName",method = RequestMethod.DELETE)
    public String deleteByUserName(String username){
        if(userDao.deleteByUserName(username)){
            return "success";
        }
        return "failure";
    }

    @RequestMapping(value = "findByUserName",method = RequestMethod.GET)
    public List<UserEntity> findByUserName(String username){
        return userDao.findByUserName(username);
    }
}
