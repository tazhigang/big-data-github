package com.ittzg.es.dao.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/24 17:15
 */
@Document(indexName="news",type = "article")
public class ArticleEntity {
    @Id
    private Long id;
    private String title;
    private String content;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
