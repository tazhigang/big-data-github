package com.ittzg.es.service;

import com.ittzg.es.dao.entity.ArticleEntity;

import java.util.List;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/24 17:40
 */
public interface ArticleService {
    // 添加或修改
    boolean saveOrModeify(ArticleEntity articleEntity);
    // 删除
    boolean delete(String title);

    // 查询所有
    List<ArticleEntity> queryAll();

}
