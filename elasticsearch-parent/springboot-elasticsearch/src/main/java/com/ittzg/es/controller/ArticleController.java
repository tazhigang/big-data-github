package com.ittzg.es.controller;

import com.ittzg.es.dao.entity.ArticleEntity;
import com.ittzg.es.service.ArticleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/24 17:27
 */
@RestController
@RequestMapping(value = "article")
public class ArticleController {

    @Autowired
    private ArticleService articleService;

    @RequestMapping(value = "addOrModify",method = RequestMethod.POST)
    public String addOrModify(@RequestBody ArticleEntity articleEntity){
        if(articleService.saveOrModeify(articleEntity)){
            return "success";
        }
        return "failure";
    }

    @RequestMapping(value = "deleteByTitle",method = RequestMethod.DELETE)
    public String deleteByTitle(String title){
        if(articleService.delete(title)){
            return "success";
        }
        return "failure";
    }

    @RequestMapping(value = "findAll",method = RequestMethod.GET)
    public List<ArticleEntity> findAll(){
        return articleService.queryAll();
    }
}
