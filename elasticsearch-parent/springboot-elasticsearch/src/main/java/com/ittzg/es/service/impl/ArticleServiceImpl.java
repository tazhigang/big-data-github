package com.ittzg.es.service.impl;

import com.ittzg.es.dao.ArticleDao;
import com.ittzg.es.dao.entity.ArticleEntity;
import com.ittzg.es.service.ArticleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/24 17:40
 */
@Service
public class ArticleServiceImpl implements ArticleService {
    @Autowired
    private ArticleDao articleDao;
    @Override
    public boolean saveOrModeify(ArticleEntity articleEntity) {
        try {
            if(articleEntity==null){
                return false;
            }
            articleDao.save(articleEntity);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean delete(String title) {
        try {
            List<ArticleEntity> articleEntities = articleDao.findAllByTitle(title);
            articleDao.deleteAll(articleEntities);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public List<ArticleEntity> queryAll() {
        List<ArticleEntity> articleEntities = new ArrayList<>();
        Iterable<ArticleEntity> entities = articleDao.findAll();
        Iterator<ArticleEntity> iterator = entities.iterator();
        while(iterator.hasNext()){
            articleEntities.add(iterator.next());
        }
        return articleEntities;
    }

}
