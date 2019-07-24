package com.ittzg.es.dao;

import com.ittzg.es.dao.entity.ArticleEntity;
import org.springframework.data.elasticsearch.repository.ElasticsearchCrudRepository;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.List;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/24 17:15
 */
@Repository
public interface ArticleDao extends ElasticsearchRepository<ArticleEntity,Long> {
    Optional<ArticleEntity> findById(Long id);

    List<ArticleEntity> findAllByTitle(String title);

}
