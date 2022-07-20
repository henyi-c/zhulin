package com.zhulin.common.es.config;


import org.apache.http.HttpHost;
import org.apache.http.util.Asserts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import java.util.List;

/*
 * @author: henyi
 * @date: 2022/7/18 17:23
 * @description: 这里进行 RestHighLevelClient 配置，用于操作 ElasticSearch。
 **/
@Configuration
public class ElasticSearchConfig {



    @Value("${common.elasticsearch.rest.scheme:http}")
    private String scheme;

    @Value("${common.elasticsearch.rest.ip-address:127.0.0.1:9200}")
    private List<String> ipAddressList;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(RestClient.builder(this.createHttpHost()));
    }


    /**
     * 创建 HttpHost 对象
     *
     * @return 返回 HttpHost 对象数组
     */
    private HttpHost[] createHttpHost() {
        Asserts.check(!CollectionUtils.isEmpty(ipAddressList), "ElasticSearch cluster ip address cannot empty");

        HttpHost[] httpHosts = new HttpHost[ipAddressList.size()];
        for (int i = 0, len = ipAddressList.size(); i < len; i++) {
            String ipAddress = ipAddressList.get(i);
            String[] values = ipAddress.split(":");

            String ip = values[0];
            int port = Integer.parseInt(values[1]);
            // 创建 HttpHost
            httpHosts[i] = new HttpHost(ip, port, scheme);
        }

        return httpHosts;

    }
}