package com.zhulin.common.es.service.impl;


import cn.hutool.core.convert.Convert;
import cn.hutool.core.lang.TypeReference;
import cn.hutool.json.JSONUtil;
import com.zhulin.common.es.entity.ES_Document;
import com.zhulin.common.es.service.BaseSearchService;
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
public class BaseSearchServiceImpl implements BaseSearchService {

    @Value("${spring.elasticsearch.default-shards:1}")
    private String ELASTIC_SEARCH_DEFAULT_SHARDS;

    @Value("${spring.elasticsearch.default-replicas:1}")
    private String ELASTIC_SEARCH_DEFAULT_REPLICAS;

    @Resource
    private RestHighLevelClient client;


    @Override
    public boolean addIndex(String index, String aliasesJsonStr, String mappingJsonStr, String settingJsonStr) throws Exception {
        // ??????????????????
        CreateIndexRequest request = new CreateIndexRequest(index);
        // ????????????
        Map<String, ?> aliasesMap = Convert.convert(new TypeReference<Map<String, ?>>() {
        }, JSONUtil.toBean(aliasesJsonStr, Map.class));
        //??????aliases??????
        request.aliases(aliasesMap);
        // ????????????
        Map<String, ?> mapMap = Convert.convert(new TypeReference<Map<String, ?>>() {
        }, JSONUtil.toBean(mappingJsonStr, Map.class));
        //??????mapping??????
        request.mapping(mapMap);
        // ????????????
        Map<String, ?> settingMap = Convert.convert(new TypeReference<Map<String, ?>>() {
        }, JSONUtil.toBean(settingJsonStr, Map.class));
        //??????setting??????
        request.settings(settingMap);
        // ???????????????ES
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        // ??????????????????
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
        // ??????????????????
        GetIndexRequest request = new GetIndexRequest(index);
        // ???????????????ES
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        // ??????????????????
        Map<String, Object> indexInfo = new HashMap<>();
        indexInfo.put("aliases", response.getAliases());
        indexInfo.put("mappings", response.getMappings());
        indexInfo.put("settings", response.getSettings());
        return indexInfo;
    }

    @Override
    public boolean deleteIndex(String index) throws Exception {
        // ??????????????????
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        // ???????????????ES
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        // ??????????????????
        return response.isAcknowledged();
    }

    @Override
    public String insertData(ES_Document<?> ES_Document) throws Exception {
        // ??????????????????
        IndexRequest request = new IndexRequest(ES_Document.getIndex());
        // ????????????id
        request.id(ES_Document.getId());
        // ???json??????????????????????????????
        request.source(JSONUtil.toJsonStr(ES_Document.getData()), XContentType.JSON);
        // ???????????????ES
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        // ??????????????????
        return response.getResult().toString();
    }


    @Override
    public String updateData(ES_Document<?> ES_Document) throws Exception {
        // ??????????????????
        UpdateRequest request = new UpdateRequest();
        request.index(ES_Document.getIndex()).id(ES_Document.getId());
        // ?????????????????????request.doc(XContentType.JSON, "name", "??????", "age", 25);
        request.doc(JSONUtil.toJsonStr(ES_Document.getData()), XContentType.JSON);
        // ???????????????ES
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        // ??????????????????
        return response.getResult().toString();
    }


    @Override
    public ES_Document<?> queryDataById(String index, String documentId) throws Exception {
        return queryDataById(index, documentId, String.class);
    }


    @Override
    public ES_Document<?> queryDataById(String index, String documentId, Class<?> clz) throws Exception {
        // ??????????????????
        GetRequest request = new GetRequest(index);
        request.id(documentId);
        // ???????????????ES
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // ??????????????????
        return new ES_Document<>(response.getIndex(), response.getId(), JSONUtil.toBean(response.getSourceAsString(), clz));
    }

    @Override
    public String deleteDataById(String index, String documentId) throws Exception {
        // ??????????????????
        DeleteRequest request = new DeleteRequest(index);
        request.id(documentId);
        // ???????????????ES
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        // ??????????????????
        return response.getResult().toString();
    }

    @Override
    public List<BulkItemResponse.Failure> deleteDataByQuery(String index, QueryBuilder queryBuilder) throws IOException {
        // ??????????????????
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        //QueryBuilders.termQuery("sex", "???") QueryBuilders.matchAllQuery()
        request.setQuery(queryBuilder);
        // ???????????????ES
        BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
        // ????????????????????????
        return response.getBulkFailures();
    }

    @Override
    public Map<String, Object> batchInsertData(List<ES_Document<?>> ES_DocumentList) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        // ???????????????????????????
        ES_DocumentList.forEach(ES_Document -> {
            // ??????????????????
            IndexRequest request = new IndexRequest(ES_Document.getIndex());
            // ??????id
            request.id(ES_Document.getId());
            // ???json??????????????????????????????
            // ????????????????????????????????????request.source(XContentType.JSON, "name", "??????", "age", "???", "age", 22);?????????"name"???"age"??? "age"???User???????????????????????????????????????????????????????????????????????????
            request.source(JSONUtil.toJsonStr(ES_Document.getData()), XContentType.JSON);
            // ???request??????????????????????????????
            bulkRequest.add(request);
        });
        // ???????????????ES
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        Map<String, Object> resMap = new HashMap<>();
        resMap.put("batchInsertFlag", response.hasFailures());
        // ??????????????????
        log.info("???????????????????????????{}", response.hasFailures());
        // ??????????????????
        // ?????????????????????id
        List<String> successDocumentId = new ArrayList<>();
        // ?????????????????????id
        List<String> failDocumentId = new ArrayList<>();
        for (BulkItemResponse itemResponse : response) {
            BulkItemResponse.Failure failure = itemResponse.getFailure();
            if (failure == null) {
                successDocumentId.add(itemResponse.getId());
                log.info("?????????????????????id???{}", itemResponse.getId());
            } else {
                failDocumentId.add(itemResponse.getId());
                log.info("?????????????????????id???{}", itemResponse.getId());
            }
        }
        resMap.put("successId", successDocumentId);
        resMap.put("failId", failDocumentId);
        return resMap;
    }

    @Override
    public Map<String, Object> batchDeleteData(List<ES_Document<?>> ES_DocumentList) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        // ???????????????????????????
        ES_DocumentList.forEach(ES_Document -> {
            DeleteRequest request = new DeleteRequest(ES_Document.getIndex());
            // ??????id
            request.id(ES_Document.getId());
            // ???request??????????????????????????????
            bulkRequest.add(request);
        });
        // ???????????????ES
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        Map<String, Object> resMap = new HashMap<>();
        resMap.put("batchDeleteFlag", response.hasFailures());
        // ??????????????????
        log.info("???????????????????????????{}", response.hasFailures());
        // ??????????????????
        // ?????????????????????id
        List<String> successDocumentId = new ArrayList<>();
        // ?????????????????????id
        List<String> failDocumentId = new ArrayList<>();
        for (BulkItemResponse itemResponse : response) {
            BulkItemResponse.Failure failure = itemResponse.getFailure();
            if (failure == null) {
                successDocumentId.add(itemResponse.getId());
                log.info("?????????????????????id???{}", itemResponse.getId());
            } else {
                failDocumentId.add(itemResponse.getId());
                log.info("?????????????????????id???{}", itemResponse.getId());
            }
        }
        resMap.put("successId", successDocumentId);
        resMap.put("failId", failDocumentId);
        return resMap;
    }


    @Override
    public List<ES_Document<?>> queryDataList(String index, SearchSourceBuilder builder, Class<?> clz) throws Exception {
        List<ES_Document<?>> ES_DocumentList = new ArrayList<>();
        // ??????????????????
        SearchRequest request = new SearchRequest();
        request.indices(index);
        // ??????????????????
        request.source(builder);
        // ???????????????ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // ??????????????????
        for (SearchHit hit : response.getHits().getHits()) {
            ES_DocumentList.add(new ES_Document<>(hit.getIndex(), hit.getId(), JSONUtil.toBean(hit.getSourceAsString(), clz)));
        }
        return ES_DocumentList;
    }

    @Override
    public List<Map<String, Object>> queryDataList(String index, SearchSourceBuilder builder) throws Exception {
        List<Map<String, Object>> mapList = new ArrayList<>();
        // ??????????????????
        SearchRequest request = new SearchRequest();
        request.indices(index);
        // ??????????????????
        request.source(builder);
        // ???????????????ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // ??????????????????
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> data = Convert.convert(new TypeReference<Map<String, Object>>() {
            }, JSONUtil.toBean(hit.getSourceAsString(), Map.class));
            data.put("index", hit.getIndex());
            data.put("id", hit.getId());
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
