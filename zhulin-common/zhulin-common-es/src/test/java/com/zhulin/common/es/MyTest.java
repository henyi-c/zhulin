package com.zhulin.common.es;

import com.zhulin.common.es.entity.ES_Document;
import com.zhulin.common.es.service.CommonSearchService;
import com.zhulin.common.es.service.impl.CommonSearchServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * 封装测试
 */
@Slf4j
@Component
public class MyTest {

    @Autowired
    CommonSearchService commonSearchService;


    /**
     * 流程测试
     */
    @PostConstruct
    public void initTest() throws Exception {
        queryDataList();
    }


    void addIndex() throws Exception {
        log.info("{}",commonSearchService.addIndex("chunxiao"));
    }

    void getIndexInfo() throws Exception {
        log.info("{}",commonSearchService.getIndexInfo("chunxiao"));
    }

    void deleteIndex() throws Exception {
        log.info("{}",commonSearchService.deleteIndex("chunxiao"));
    }

    void insertDataToIndex() throws Exception {
        log.info("{}",commonSearchService.insertData(new ES_Document<String>("chunxiao","{\n" +
                "    \"name\": \"JsonT\",\n" +
                "    \"description\": \"一个简洁的在线 JSON 解析器\",\n" +
                "    \"features\": [\n" +
                "        {\n" +
                "            \"name\": \"一键分享\",\n" +
                "            \"available\": true\n" +
                "        }\n" +
                "    ]\n" +
                "}")));
    }


    void queryDataList() throws Exception {
        log.info("{}",commonSearchService.queryDataPage(1,5,"chunxiao", new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())));
    }
}
