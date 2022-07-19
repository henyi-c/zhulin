package com.zhulin.common.es.service.impl;

import cn.hutool.json.JSONUtil;
import com.zhulin.common.es.entity.ElasticSearchDocument;
import com.zhulin.common.es.service.BaseSearchService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.zhulin.common.es.constant.ElasticSearchConst.ELASTIC_SEARCH_TYPE;

@Slf4j
@Service
public class BaseSearchServiceImpl<T> implements BaseSearchService<T> {


    @Value("${spring.elasticsearch.default-shards:1}")
    private String ELASTIC_SEARCH_DEFAULT_SHARDS;

    @Value("${spring.elasticsearch.default-replicas:0}")
    private List<String> ELASTIC_SEARCH_DEFAULT_REPLICAS;


    @Resource
    private RestHighLevelClient restHighLevelClient;


    @Override
    public boolean createIndex(String index, Map<String, Map<String, Object>> properties) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        // ES 7.0 后的版本中，已经弃用 type
        builder.startObject()
                .startObject("mappings")
                .field("properties", properties)
                .endObject()
                .startObject("settings")
                .field("number_of_shards", ELASTIC_SEARCH_DEFAULT_SHARDS)
                .field("number_of_replicas", ELASTIC_SEARCH_DEFAULT_REPLICAS)
                .endObject()
                .endObject();

        CreateIndexRequest request = new CreateIndexRequest(index).source(builder);
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        return response.isAcknowledged();
    }

    @Override
    public boolean isExistIndex(String index) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        getIndexRequest.local(false);
        getIndexRequest.humanReadable(true);
        getIndexRequest.includeDefaults(false);

        return restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }

    @Override
    public boolean deleteIndex(String index) throws IOException {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                throw new ElasticsearchException("Not found index: " + index);
            }
            throw exception;
        }
    }

    @Override
    public void save(String index, ElasticSearchDocument<T> document) throws IOException {
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.id(document.getId());
        indexRequest.source(JSONUtil.toJsonStr(document.getData()), XContentType.JSON);
        // 保存文档数据
        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

    }

    @Override
    public void saveAll(String index, List<ElasticSearchDocument<T>> elasticSearchDocuments) throws IOException {
        if (CollectionUtils.isEmpty(elasticSearchDocuments)) {
            return;
        }
        // 批量请求
        BulkRequest bulkRequest = new BulkRequest();
        elasticSearchDocuments.forEach(doc -> bulkRequest.add(new IndexRequest(index)
                .id(doc.getId())
                .source(JSONUtil.toJsonStr(doc.getData()), XContentType.JSON)));

        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @Override
    public void delete(String index, String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, id);
        restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    @Override
    public void deleteByQuery(String index, QueryBuilder queryBuilder) throws IOException {
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(index).setQuery(queryBuilder);
        deleteRequest.setConflicts("proceed");

        restHighLevelClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
    }

    @Override
    public void deleteAll(String index, List<String> idList) throws IOException {
        if (CollectionUtils.isEmpty(idList)) {
            return;
        }
        BulkRequest bulkRequest = new BulkRequest();
        idList.forEach(id -> bulkRequest.add(new DeleteRequest(index, id)));

        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @Override
    public <t> t get(String index, String id, Class<t> resultType) throws IOException {
        GetRequest getRequest = new GetRequest(index, id);
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        String resultAsString = response.getSourceAsString();
        return JSONUtil.toBean(resultAsString, resultType);
    }

    @Override
    public <t> List<t> searchByQuery(String index, SearchSourceBuilder sourceBuilder, Class<t> resultType) throws IOException {
        // 构建查询请求
        SearchRequest searchRequest = new SearchRequest(index).source(sourceBuilder);
        // 获取返回值
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        // 创建空的查询结果集合
        List<t> results = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            // 以字符串的形式获取数据源
            String sourceAsString = hit.getSourceAsString();
            results.add(JSONUtil.toBean(sourceAsString, resultType));
        }

        return results;

    }

    @Override
    public void updateById(Map<String, Object> data, String indexName, String id) {
        log.info("es开始更新数据:{}", JSONUtil.toJsonStr(data));
        try {
            UpdateRequest request = new UpdateRequest(indexName, ELASTIC_SEARCH_TYPE, id).doc(data);
            UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
            log.info("更新状态：{}", response.getResult());
        } catch (IOException e) {
            log.error("更新写入异常:{}", e.getMessage(), e);
        }
        if (log.isDebugEnabled()) {
            log.info("es更新数据完成");
        }
    }





}