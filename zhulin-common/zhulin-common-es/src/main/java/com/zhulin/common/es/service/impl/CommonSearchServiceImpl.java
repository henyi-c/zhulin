package com.zhulin.common.es.service.impl;


import cn.hutool.core.convert.Convert;
import cn.hutool.core.lang.TypeReference;
import cn.hutool.json.JSONUtil;
import com.zhulin.common.es.entity.ES_Document;
import com.zhulin.common.es.service.CommonSearchService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class CommonSearchServiceImpl implements CommonSearchService {

    @Value("${spring.elasticsearch.default-shards:1}")
    private String ELASTIC_SEARCH_DEFAULT_SHARDS;

    @Value("${spring.elasticsearch.default-replicas:1}")
    private String ELASTIC_SEARCH_DEFAULT_REPLICAS;

    @Resource
    private RestHighLevelClient client;


    @Override
    public boolean addIndex(String index, String aliasesJsonStr, String mappingJsonStr, String settingJsonStr) throws Exception {
        // 定义索引名称
        CreateIndexRequest request = new CreateIndexRequest(index);
        // 转换类型
        Map<String, ?> aliasesMap = Convert.convert(new TypeReference<Map<String, ?>>() {
        }, JSONUtil.toBean(aliasesJsonStr, Map.class));
        //添加aliases数据
        request.aliases(aliasesMap);
        // 转换类型
        Map<String, ?> mapMap = Convert.convert(new TypeReference<Map<String, ?>>() {
        }, JSONUtil.toBean(mappingJsonStr, Map.class));
        //添加mapping数据
        request.mapping(mapMap);
        // 转换类型
        Map<String, ?> settingMap = Convert.convert(new TypeReference<Map<String, ?>>() {
        }, JSONUtil.toBean(settingJsonStr, Map.class));
        //添加setting数据
        request.settings(settingMap);
        // 发送请求到ES
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        // 处理响应结果
        return response.isAcknowledged();
    }

    @Override
    public boolean addIndex(String index) throws Exception {
        return addIndex(index, "{\"" + index + ".aliases\":{}}", "{\"properties\":{}}", "{\"index\":{\"number_of_shards\":\"" + ELASTIC_SEARCH_DEFAULT_SHARDS + "\",\"number_of_replicas\":\"" + ELASTIC_SEARCH_DEFAULT_REPLICAS + "\"}}");
    }

    @Override
    public boolean isExistIndex(String index) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        getIndexRequest.local(false);
        getIndexRequest.humanReadable(true);
        getIndexRequest.includeDefaults(false);
        return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }


    @Override
    public Map<String, Object> getIndexInfo(String index) throws Exception {
        // 定义索引名称
        GetIndexRequest request = new GetIndexRequest(index);
        // 发送请求到ES
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        // 处理响应结果
        Map<String, Object> indexInfo = new HashMap<>();
        indexInfo.put("aliases", response.getAliases());
        indexInfo.put("mappings", response.getMappings());
        indexInfo.put("settings", response.getSettings());
        return indexInfo;
    }

    @Override
    public boolean deleteIndex(String index) throws Exception {
        // 定义索引名称
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        // 发送请求到ES
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        // 处理响应结果
        return response.isAcknowledged();
    }

    @Override
    public String insertData(ES_Document<?> ES_Document) throws Exception {
        // 定义请求对象
        IndexRequest request = new IndexRequest(ES_Document.getIndex());
        // 设置文档id
        request.id(ES_Document.getDocumentId());
        // 将json格式字符串放在请求中
        request.source(JSONUtil.toJsonStr(ES_Document.getData()), XContentType.JSON);
        // 发送请求到ES
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        // 处理响应结果
        return response.getResult().toString();
    }


    @Override
    public String updateData(ES_Document<?> ES_Document) throws Exception {
        // 定义请求对象
        UpdateRequest request = new UpdateRequest();
        request.index(ES_Document.getIndex()).id(ES_Document.getDocumentId());
        // 也可以这样写：request.doc(XContentType.JSON, "name", "李四", "age", 25);
        request.doc(JSONUtil.toJsonStr(ES_Document.getData()), XContentType.JSON);
        // 发送请求到ES
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        // 处理响应结果
        return response.getResult().toString();
    }


    @Override
    public ES_Document<?> getDataById(String index, String documentId) throws Exception {
        return getDataById(index, documentId, String.class);
    }


    @Override
    public ES_Document<?> getDataById(String index, String documentId, Class<?> clz) throws Exception {
        // 定义请求对象
        GetRequest request = new GetRequest(index);
        request.id(documentId);
        // 发送请求到ES
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 处理响应结果
        return new ES_Document<>(response.getIndex(), response.getId(), JSONUtil.toBean(response.getSourceAsString(), clz));
    }

    @Override
    public String deleteDataById(String index, String documentId) throws Exception {
        // 定义请求对象
        DeleteRequest request = new DeleteRequest(index);
        request.id(documentId);
        // 发送请求到ES
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        // 处理响应结果
        return response.getResult().toString();
    }

    @Override
    public List<BulkItemResponse.Failure> deleteDataByQuery(String index, QueryBuilder queryBuilder) throws IOException {
        // 定义请求对象
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        //QueryBuilders.termQuery("sex", "男") QueryBuilders.matchAllQuery()
        request.setQuery(queryBuilder);
        // 发送请求到ES
        BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
        // 返回删除失败结果
        return response.getBulkFailures();
    }

    @Override
    public Map<String, Object> batchInsertData(List<ES_Document<?>> ES_DocumentList) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        // 准备批量插入的数据
        ES_DocumentList.forEach(ES_Document -> {
            // 设置请求对象
            IndexRequest request = new IndexRequest(ES_Document.getIndex());
            // 文档id
            request.id(ES_Document.getDocumentId());
            // 将json格式字符串放在请求中
            // 下面这种写法也可以写成：request.source(XContentType.JSON, "name", "张三", "age", "男", "age", 22);，其中"name"、"age"、 "age"是User对象中的字段名，而这些字段名称后面的值就是对应的值
            request.source(JSONUtil.toJsonStr(ES_Document.getData()), XContentType.JSON);
            // 将request添加到批量处理请求中
            bulkRequest.add(request);
        });
        // 发送请求到ES
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        Map<String, Object> resMap = new HashMap<>();
        resMap.put("batchInsertFlag", response.hasFailures());
        // 处理响应结果
        log.info("批量插入是否失败：{}", response.hasFailures());
        // 插入详细信息
        // 插入成功的文档id
        List<String> successDocumentId = new ArrayList<>();
        // 插入失败的文档id
        List<String> failDocumentId = new ArrayList<>();
        for (BulkItemResponse itemResponse : response) {
            BulkItemResponse.Failure failure = itemResponse.getFailure();
            if (failure == null) {
                successDocumentId.add(itemResponse.getId());
                log.info("插入成功的文档id：{}", itemResponse.getId());
            } else {
                failDocumentId.add(itemResponse.getId());
                log.info("插入失败的文档id：{}", itemResponse.getId());
            }
        }
        resMap.put("successDocumentId", successDocumentId);
        resMap.put("failDocumentId", failDocumentId);
        return resMap;
    }

    @Override
    public Map<String, Object> batchDeleteData(List<ES_Document<?>> ES_DocumentList) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        // 准备批量删除的数据
        ES_DocumentList.forEach(ES_Document -> {
            DeleteRequest request = new DeleteRequest(ES_Document.getIndex());
            // 文档id
            request.id(ES_Document.getDocumentId());
            // 将request添加到批量处理请求中
            bulkRequest.add(request);
        });
        // 发送请求到ES
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        Map<String, Object> resMap = new HashMap<>();
        resMap.put("batchDeleteFlag", response.hasFailures());
        // 处理响应结果
        log.info("批量删除是否失败：{}", response.hasFailures());
        // 插入详细信息
        // 插入成功的文档id
        List<String> successDocumentId = new ArrayList<>();
        // 插入失败的文档id
        List<String> failDocumentId = new ArrayList<>();
        for (BulkItemResponse itemResponse : response) {
            BulkItemResponse.Failure failure = itemResponse.getFailure();
            if (failure == null) {
                successDocumentId.add(itemResponse.getId());
                log.info("删除成功的文档id：{}", itemResponse.getId());
            } else {
                failDocumentId.add(itemResponse.getId());
                log.info("删除失败的文档id：{}", itemResponse.getId());
            }
        }
        resMap.put("successDocumentId", successDocumentId);
        resMap.put("failDocumentId", failDocumentId);
        return resMap;
    }


    @Override
    public List<ES_Document<?>> queryDataList(String index, SearchSourceBuilder builder, Class<?> clz) throws Exception {
        List<ES_Document<?>> ES_DocumentList = new ArrayList<>();
        // 定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices(index);
        // 指定检索条件
        request.source(builder);
        // 发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            ES_DocumentList.add(new ES_Document<>(hit.getIndex(), hit.getId(), JSONUtil.toBean(hit.getSourceAsString(), clz)));
        }
        return ES_DocumentList;
    }

    @Override
    public List<Map<String, Object>> queryDataList(String index, SearchSourceBuilder builder) throws Exception {
        List<Map<String, Object>> mapList = new ArrayList<>();
        // 定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices(index);
        // 指定检索条件
        request.source(builder);
        // 发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> data = Convert.convert(new TypeReference<Map<String, Object>>() {
            }, JSONUtil.toBean(hit.getSourceAsString(), Map.class));
            data.put("index", hit.getIndex());
            data.put("documentId", hit.getId());
            mapList.add(data);
        }
        return mapList;
    }


    @Override
    public List<ES_Document<?>> queryDataPage(int currentPage, int pageSize, String index, SearchSourceBuilder builder, Class<?> clz) throws Exception {
        int from = (currentPage - 1) * pageSize;
        builder.from(from);
        builder.size(pageSize);
        return queryDataList(index, builder, clz);
    }


    @Override
    public List<Map<String, Object>> queryDataPage(int currentPage, int pageSize, String index, SearchSourceBuilder builder) throws Exception {
        int from = (currentPage - 1) * pageSize;
        builder.from(from);
        builder.size(pageSize);
        return queryDataList(index, builder);
    }


}
