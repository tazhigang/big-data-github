package com.ittzg.es;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/21 22:52
 */
public class ElasticSearchCientTest {
    private TransportClient client;

    @Before
    public void init() throws UnknownHostException {
        // 1 设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();

        // 2 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.0.120"), 9300));
    }
    @After
    public void destroy(){
        if(client!=null){
            // 关闭连接
            client.close();
        }
    }
    @Test
    public void printClient(){
        // 打印集群名称
        System.out.println(client.toString());
    }

    /**
     * 创建索引
     * http://192.168.0.120:9200/blog 查看
     */
    @Test
    public void createIndex_blog(){
        // 1 创建索引
        CreateIndexResponse response = client.admin().indices().prepareCreate("blog2").get();
        System.out.println(JSON.toJSONString(response));
    }
    /**
     * 删除索引
     */
    @Test
    public void deleteIndex(){
        // 1 删除索引
        client.admin().indices().prepareDelete("blog").get();
    }

    /**
     * 新建文档 json形式添加
     */
    @Test
    public void createIndexByJson() {

        // 1 文档数据准备
        String json = "{\"id\":1,\"title\":\"基于Lucene的搜索服务器\",\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"}";
        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "1").setSource(json).execute().actionGet();

        // 3 打印返回的结果
        System.out.println(JSON.toJSONString(indexResponse));
    }

    /**
     * 新建文档 map形式添加
     */
    @Test
    public void createIndexByMap() {

        // 1 文档数据准备
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("id", "2");
        json.put("title", "基于Lucene的搜索服务器");
        json.put("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");

        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "2").setSource(json).execute().actionGet();

        // 3 打印返回的结果
        System.out.println(JSON.toJSONString(indexResponse));
    }

    /**
     * 使用es自带的工具类，构建json数据 添加文档
     * @throws Exception
     */
    @Test
    public void createIndex() throws Exception {

        // 1 通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id", 3)
                .field("title", "基于Lucene的搜索服务器").field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。")
                .endObject();

        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "3").setSource(builder).get();

        // 3 打印返回的结果
        System.out.println(JSON.toJSONString(indexResponse));

    }

    /**
     * 根据单个索引查询
     * @throws Exception
     */
    @Test
    public void getData() throws Exception {

        // 1 查询文档
        GetResponse response = client.prepareGet("blog", "article", "1").get();

        // 2 打印搜索的结果
        System.out.println(response.getSourceAsString());
    }

    /**
     * 根据多个索引查询
     * @throws Exception
     */
    @Test
    public void getMultiData() {

        // 1 查询多个文档
        MultiGetResponse response = client.prepareMultiGet().add("blog", "article", "1").add("blog", "article", "2", "3")
                .add("blog", "article", "2").get();

        // 2 遍历返回的结果
        for(MultiGetItemResponse itemResponse:response){
            GetResponse getResponse = itemResponse.getResponse();

            // 如果获取到查询结果
            if (getResponse.isExists()) {
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }
    }

    /**
     * 更新文档
     * @throws Exception
     */
    @Test
    public void testUpsert() throws Exception {

        // 设置查询条件, 查找不到则添加
        IndexRequest indexRequest = new IndexRequest("blog", "article", "5")
                .source(XContentFactory.jsonBuilder().startObject()
                        .field("title", "搜索服务器")
                        .field("content","它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                        .endObject());

        // 设置更新, 查找到更新下面的设置
        UpdateRequest upsert = new UpdateRequest("blog", "article", "5")
                .doc(XContentFactory.jsonBuilder().startObject().field("user", "王五").field("id","5").endObject()).upsert(indexRequest);

        client.update(upsert).get();
    }

    /**
     * 删除文档
     */
    @Test
    public void deleteData() {

        // 1 删除文档数据
        DeleteResponse indexResponse = client.prepareDelete("blog", "article", "5").get();

        // 2 打印返回的结果
        System.out.println(JSON.toJSONString(indexResponse));
    }
    /********************************************* 下面是查询操作***************************************************/
    /**
     * 查询所有
     */
    @Test
    public void matchAllQuery() {

        // 1 执行查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery()).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象

            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
    }

    /**
     * 对所有字段分词查询
     */
    @Test
    public void query() {
        // 1 条件查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("一个")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象

            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
    }
    /**
     * 通配符查询
     * "*"：表示多个字符（任意的字符）
     * "？"：表示单个字符
     */
    @Test
    public void wildcardQuery() {

        // 1 通配符查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.wildcardQuery("content", "*全*")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象

            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
    }

    /**
     * 词条查询
     */
    @Test
    public void termQuery() {

        // 1 第一field查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "全")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象

            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
    }

    /**
     * 模糊查询
     */
    @Test
    public void fuzzy() {

        // 1 模糊查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.fuzzyQuery("title", "lucene")).get();

        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象

            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
    }
    /**
     * 映射相关操作
     */
    @Test
    public void createMapping() throws Exception {

        // 1设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id1")
                                .field("type", "string")
                                .field("store", "yes")
                            .endObject()
                            .startObject("title2")
                                .field("type", "string")
                                .field("store", "no")
                            .endObject()
                            .startObject("content")
                                .field("type", "string")
                                .field("store", "yes")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        // 2 添加mapping
        PutMappingRequest mapping = Requests.putMappingRequest("blog2").type("article").source(builder);

        client.admin().indices().putMapping(mapping).get();

    }


}
