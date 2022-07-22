package com.zhulin.common.es;

import com.zhulin.common.es.entity.ES_Document;
import com.zhulin.common.es.service.BaseSearchService;
import com.zhulin.common.es.utils.ES_HelpUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * 封装测试
 */
@Slf4j
@Component
public class MyTest {

    @Autowired
    BaseSearchService baseSearchService;


    /**
     * 流程测试
     */
    @PostConstruct
    public void initTest() throws Exception {
        queryDataList();
    }


    void addIndex() throws Exception {
        log.info("{}", baseSearchService.addIndex("chunxiao"));
    }

    void getIndexInfo() throws Exception {
        log.info("{}", baseSearchService.getIndexInfo("chunxiao"));
    }

    void deleteIndex() throws Exception {
        log.info("{}", baseSearchService.deleteIndex("chunxiao"));
    }

    void insertDataToIndex() throws Exception {
        log.info("{}", baseSearchService.insertData(new ES_Document<>("chunxiao", "{\n" +
                "    \"name\": \"json kkay\",\n" +
                "    \"age\": 18,\n" +
                "    \"description\": \"一个简洁的在线 JSON 解析器\",\n" +
                "    \"features\": [\n" +
                "        {\n" +
                "            \"name\": \"一键分享\",\n" +
                "            \"available\": true\n" +
                "        }\n" +
                "    ]\n" +
                "}")));
    }


    void queryAll() throws Exception {
        SearchSourceBuilder query = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        log.info("{}", baseSearchService.queryDataPage(1, 5, "chunxiao", query));
    }

    void queryDataList() throws Exception {
        SearchSourceBuilder query = new SearchSourceBuilder().query(ES_HelpUtil.like("description", "S"));
        log.info("{}", baseSearchService.queryDataPage(1, 5, "chunxiao", query));
    }

    void queryDataById() throws Exception {
        log.info("{}", baseSearchService.queryDataById("chunxiao", "XUzTH4IB-Hu_4rgLoYDj"));
    }


}
