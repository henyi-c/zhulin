import cn.hutool.json.JSONUtil;
import com.zhulin.common.es.entity.ElasticSearchDocument;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;



/*
 * @author: henyi
 * @date: 2022/7/19 10:37
 * @description: 常用方法合集
 **/

public class TestApp {


    /*
     * @author: henyi
     * @date: 2022/7/19 11:11
     * @description: 添加索引结构到es
     **/
    @Test
    void addIndex() throws Exception {
        // 创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 定义索引名称
        CreateIndexRequest request = new CreateIndexRequest("user");
        // 添加aliases，对比上述结构来理解
        String aliaseStr = "{\"user.aliases\":{}}";
        Map aliases = JSONUtil.toBean(aliaseStr, Map.class);
        // 添加mappings，对比上述结构来理解
        String mappingStr = "{\"properties\":{\"name\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}},\"sex\":{\"type\":\"keyword\"},\"age\":{\"type\":\"integer\"}}}";
        Map mappings = JSONUtil.toBean(mappingStr, Map.class);
        // 添加settings，对比上述结构来理解
        String settingStr = "{\"index\":{\"number_of_shards\":\"9\",\"number_of_replicas\":\"2\"}}";
        Map settings = JSONUtil.toBean(settingStr, Map.class);

        // 添加数据
        request.aliases(aliases);
        request.mapping(mappings);
        request.settings(settings);

        // 发送请求到ES
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        // 处理响应结果
        System.out.println("添加索引是否成功：" + response.isAcknowledged());
        // 关闭ES客户端对象
        client.close();
    }


    /*
     * @description: 获取索引信息
     * @author: henyi
     * @date: 2022/7/19 10:38
     * @param:
     * @return:
     **/
    @Test
    void getIndexInfo() throws Exception {
        // 创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 定义索引名称
        GetIndexRequest request = new GetIndexRequest("user");
        // 发送请求到ES
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        // 处理响应结果
        System.out.println("aliases：" + response.getAliases());
        System.out.println("mappings：" + response.getMappings());
        System.out.println("settings：" + response.getSettings());
        // 关闭ES客户端对象
        client.close();
    }

    /*
     * @description: 删除索引
     * @author: henyi
     * @date: 2022/7/19 10:38
     * @param:
     * @return:
     **/
    void deleteIndex() throws Exception {
        // 创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 定义索引名称
        DeleteIndexRequest request = new DeleteIndexRequest("user");
        // 发送请求到ES
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        // 处理响应结果
        System.out.println("删除是否成功：" + response.isAcknowledged());
        // 关闭ES客户端对象
        client.close();
    }


    /*
     * @description:添加数据到索引中
     * @author: henyi
     * @date: 2022/7/19 10:39
     * @param:
     * @return:
     **/
    @Test
    void insertDataToIndex() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、创建请求对象
        ElasticSearchDocument elasticSearchDocument = new ElasticSearchDocument();
        elasticSearchDocument.setId("1");
        elasticSearchDocument.setData("{}");
        // 定义请求对象
        IndexRequest request = new IndexRequest("elasticSearchDocument");
        // 设置文档id
        request.id(elasticSearchDocument.getId());
        // 将json格式字符串放在请求中
        request.source(JSONUtil.toJsonStr(elasticSearchDocument.getData()), XContentType.JSON);
        // 3、发送请求到ES
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        System.out.println("数据插入结果：" + response.getResult());
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 10:42
     * @description: 局部更新索引中的数据
     **/
    @Test
    void updateDataFromIndex() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        ElasticSearchDocument elasticSearchDocument = new ElasticSearchDocument();
        elasticSearchDocument.setId("1");
        elasticSearchDocument.setData("{\"name\":\"小明\"}");
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("1000");
        // 拓展：局部更新也可以这样写：request.doc(XContentType.JSON, "name", "李四", "age", 25);，其中"name"和"age"是User对象中的字段名称，而"小美"和20是对应的字段值
        request.doc(JSONUtil.toJsonStr(elasticSearchDocument.getData()), XContentType.JSON);
        // 3、发送请求到ES
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        System.out.println("数据更新结果：" + response.getResult());
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 10:44
     * @description:  根据文档id查询索引中的数据
     **/
    @Test
    void getDataById() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        GetRequest request = new GetRequest("user");
        request.id("1000");
        // 3、发送请求到ES
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        System.out.println("查询结果：" + response.getSourceAsString());
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @description:
     * @author: henyi
     * @date: 2022/7/19 10:44
     * @param:
     * @return: 根据文档id删除索引中的数据
     **/
    @Test
    void deleteDataById() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        DeleteRequest request = new DeleteRequest("user");
        request.id("1000");
        // 3、发送请求到ES
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        System.out.println("删除是否成功：" + response.getResult());
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 10:45
     * @description: 根据查询条件删除索引中的数据
     **/
    @Test
    void deleteDataByQuery() throws IOException {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        DeleteByQueryRequest request = new DeleteByQueryRequest("user");
        request.setQuery(QueryBuilders.matchAllQuery());
        // 3、发送请求到ES
        BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        System.out.println("删除失败结果：" + response.getBulkFailures());
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 10:46
     * @description: 批量插入数据到索引中
     **/
    @Test
    void batchInsertDataToIndex() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        // 准备测试数据
        List<ElasticSearchDocument> elasticSearchDocumentList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            ElasticSearchDocument elasticSearchDocument = new ElasticSearchDocument();
            elasticSearchDocument.setId(i + "");
            elasticSearchDocument.setData("{\"name\":\"小明\"}");
            elasticSearchDocumentList.add(elasticSearchDocument);
        }
        BulkRequest bulkRequest = new BulkRequest();
        // 准备批量插入的数据
        elasticSearchDocumentList.forEach(elasticSearchDocument -> {
            // 设置请求对象
            IndexRequest request = new IndexRequest("user");
            // 文档id
            request.id(elasticSearchDocument.getId());
            // 将json格式字符串放在请求中
            // 下面这种写法也可以写成：request.source(XContentType.JSON, "name", "张三", "age", "男", "age", 22);，其中"name"、"age"、 "age"是User对象中的字段名，而这些字段名称后面的值就是对应的值
            request.source(JSONUtil.toJsonStr(elasticSearchDocument.getData()), XContentType.JSON);
            // 将request添加到批量处理请求中
            bulkRequest.add(request);
        });
        // 3、发送请求到ES
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        // 4、处理响应结果
        System.out.println("批量插入是否失败：" + response.hasFailures());
        // 4.1、插入详细信息
        for (BulkItemResponse itemResponse : response) {
            BulkItemResponse.Failure failure = itemResponse.getFailure();
            if (failure == null) {
                System.out.println("插入成功的文档id：" + itemResponse.getId());
            } else {
                System.out.println("插入失败的文档id：" + itemResponse.getId());
            }
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 10:49
     * @description: 批量删除索引中的数据
     **/
    @Test
    void batchDeleteDataFromIndex() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        // 准备测试数据（只用到了age来生成文档id，但是为了和上面的批量插入应和，所以需要这样做）
        List<ElasticSearchDocument> elasticSearchDocumentList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            ElasticSearchDocument elasticSearchDocument = new ElasticSearchDocument();
            elasticSearchDocument.setId(i + "");
            elasticSearchDocument.setData("{\"name\":\"小明\"}");
            elasticSearchDocumentList.add(elasticSearchDocument);
        }
        BulkRequest bulkRequest = new BulkRequest();
        // 准备批量插入的数据
        elasticSearchDocumentList.forEach(elasticSearchDocument -> {
            // 设置请求对象
            DeleteRequest request = new DeleteRequest("user");
            // 文档id
            request.id(elasticSearchDocument.getId());
            // 将request添加到批量处理请求中
            bulkRequest.add(request);
        });
        // 3、发送请求到ES
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        // 4、处理响应结果
        System.out.println("批量删除是否失败：" + response.hasFailures());
        // 4.1、删除详细信息
        for (BulkItemResponse itemResponse : response) {
            BulkItemResponse.Failure failure = itemResponse.getFailure();
            if (failure == null) {
                System.out.println("删除成功的文档id：" + itemResponse.getId());
            } else {
                System.out.println("删除失败的文档id：" + itemResponse.getId());
            }
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 10:51
     * @description: 高级查询之查询全部数据
     **/
    @Test
    void advancedQueryFromAllData() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 用来查询索引中全部的数据
        builder.query(QueryBuilders.matchAllQuery());
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<?, ?> data = JSONUtil.toBean(hit.getSourceAsString(), Map.class);
            System.out.println("文档id" + hit.getId() + "；内容：" + data);
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 10:58
     * @description: 高级查询之term精准匹配
     **/
    @Test
    void advancedQueryByTerm() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 用来查询sex是男的数据
        builder.query(QueryBuilders.termQuery("sex", "男"));
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<?, ?> data = JSONUtil.toBean(hit.getSourceAsString(), Map.class);
            System.out.println("文档id" + hit.getId() + "；内容：" + data);
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 10:59
     * @description: 高级查询之分页查询
     **/
    @Test
    void advancedQueryByPage() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 分页查询数据，本次测试只查询前5条
        builder.query(QueryBuilders.matchAllQuery());
        int currentPage = 1;
        int pageSize = 5;
        int from = (currentPage - 1) * pageSize;
        builder.from(from);
        builder.size(pageSize);
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<?, ?> data = JSONUtil.toBean(hit.getSourceAsString(), Map.class);
            System.out.println("文档id" + hit.getId() + "；内容：" + data);
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:00
     * @description: 高级查询之排序查询
     **/
    @Test
    void advancedQueryBySort() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        // 根据年龄做降序排序
        builder.sort("age", SortOrder.DESC);
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<?, ?> data = JSONUtil.toBean(hit.getSourceAsString(), Map.class);
            System.out.println("文档id" + hit.getId() + "；内容：" + data);
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:00
     * @description: 高级查询之source获取部分字段内容
     **/
    @Test
    void advancedQueryBySource() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        // 如果查询的属性很少，那就使用includes，而excludes设置为空数组
        // 如果排序的属性很少，那就使用excludes，而includes设置为空数组
        String[] includes = {"name", "age"};
        String[] excludes = {};
        builder.fetchSource(includes, excludes);
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<?, ?> data = JSONUtil.toBean(hit.getSourceAsString(), Map.class);
            System.out.println("文档id" + hit.getId() + "；内容：" + data);
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:01
     * @description: 高级查询之should匹配
     **/
    @Test
    void advancedQueryByShould() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(QueryBuilders.matchQuery("age", 30));
        // 查询中boost默认是1，写成10可以增大score比分
        boolQueryBuilder.should(QueryBuilders.matchQuery("sex", "女").boost(10));
        builder.query(boolQueryBuilder);
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<?, ?> data = JSONUtil.toBean(hit.getSourceAsString(), Map.class);
            System.out.println("文档id" + hit.getId() + "；内容：" + data);
        }
        // 5、关闭ES客户端对象
        client.close();
    }

    /*
     * @author: henyi
     * @date: 2022/7/19 11:02
     * @description: 高级查询之filter过滤查询
     **/
    @Test
    void advancedQueryByFilter() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 查询年龄大于等于26，小于等于29的结果
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("age").gte(26).lte(29));
        builder.query(boolQueryBuilder);
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<?, ?> data = JSONUtil.toBean(hit.getSourceAsString(), Map.class);
            System.out.println("文档id" + hit.getId() + "；内容：" + data);
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:03
     * @description: 高级查询之模糊查询
     **/
    @Test
    void advancedQueryByLike() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 和分词无关，这就是和mysql中like类似的做法
        // 查询名称中包含“张三”的数据，或者比“张三”多一个字符的数据，这是通过Fuzziness.ONE来控制的，比如“张三1”是可以出现的，但是“张三12”是无法出现的，这是因为他比张三多了两个字符；除了“Fuzziness.ONE”之外，还可以是“Fuzziness.TWO”等
        builder.query(QueryBuilders.fuzzyQuery("name.keyword", "张三").fuzziness(Fuzziness.ONE));
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        for (SearchHit hit : response.getHits().getHits()) {
            Map<?, ?> data = JSONUtil.toBean(hit.getSourceAsString(), Map.class);
            System.out.println("文档id" + hit.getId() + "；内容：" + data);
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:03
     * @description: 高级查询之高亮查询
     **/
    @Test
    void advancedQueryByHighLight() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 设置查询条件
        builder.query(QueryBuilders.matchPhraseQuery("name", "张三"));
        // 构建高亮查询对象
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        // 前置标签
        highlightBuilder.preTags("<b style='color:red'>");
        // 后置标签
        highlightBuilder.postTags("</b>");
        // 添加高亮的属性名称
        highlightBuilder.field("name");
        builder.highlighter(highlightBuilder);
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            Map<String, HighlightField> map = hit.getHighlightFields();
            HighlightField highlightField = map.get("name");
            System.out.println("高亮名称：" + highlightField.getFragments()[0].string());
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:04
     * @description: 高级查询之最大值聚合查询
     **/
    @Test
    void advancedQueryByMaxValueAggregation() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 获取最大年龄
        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
        builder.aggregation(aggregationBuilder);
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        Aggregations aggregations = response.getAggregations();
        ParsedMax maxAge = aggregations.get("maxAge");
        System.out.println("最大年龄：" + maxAge.getValue());
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:04
     * @description: 高级查询之分组聚合查询
     **/
    @Test
    void advancedQueryByGroupAggregation() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 按照性别分组，聚合操作要求和分词操作无关，由于sex在默认添加的时候是text类型，因为需要设置为keyword类型
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("termsSex").field("sex.keyword");
        builder.aggregation(aggregationBuilder);
        request.source(builder);
        // 3、发送请求到ES
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        Aggregations aggregations = response.getAggregations();
        // 至于使用ParsedStringTerms、ParsedLongTerms、ParsedMax、ParsedNested、ParsedAvg……是由聚合要求和聚合字段类型确定的，比如本次要求是分组，并且聚合字段是sex，那就是String类型，所以使用ParsedStringTerms
        ParsedStringTerms termsSex = aggregations.get("termsSex");
        for (Terms.Bucket bucket : termsSex.getBuckets()) {
            System.out.println("性别：" + bucket.getKeyAsString() + "；数量：" + bucket.getDocCount());
        }
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:05
     * @description: 根据查询条件计算数据量
     **/
    @Test
    void count() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        CountRequest request = new CountRequest();
        request.indices("user");
        // 指定检索条件
        request.query(QueryBuilders.matchAllQuery());
        // 如果索引不存在，不会报错
        request.indicesOptions(IndicesOptions.fromOptions(true, true, false, false));
        // 3、发送请求到ES
        CountResponse response = client.count(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        System.out.println("数据总量：" + response.getCount());
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:05
     * @description: 根据查询条件滚动查询
     **/
    @Test
    void scrollQuery() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        // 假设用户想获取第10页数据，其中每页20条
        int totalPage = 10;
        int size = 20;
        SearchRequest searchRequest = new SearchRequest("user");
        // 指定检索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        searchRequest.source(builder.query(QueryBuilders.matchAllQuery()).size(size));
        String scrollId = null;
        // 3、发送请求到ES
        SearchResponse scrollResponce = null;
        // 设置游标id存活时间
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(2));
        // 记录所有游标id
        List<String> scrollIds = new ArrayList<>();
        for (int i = 0; i < totalPage; i++) {
            try {
                // 首次检索
                if (i == 0) {
                    //记录游标id
                    searchRequest.scroll(scroll);
                    // 首次查询需要指定索引名称和查询条件
                    SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
                    // 下一次搜索要用到该游标id
                    scrollId = response.getScrollId();
                    // 记录所有游标id
                    scrollIds.add(scrollId);
                }
                // 非首次检索
                else {
                    // 不需要在使用其他条件，也不需要指定索引名称，只需要使用执行游标id存活时间和上次游标id即可，毕竟信息都在上次游标id里面呢
                    SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                    searchScrollRequest.scroll(scroll);
                    scrollResponce = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                    // 下一次搜索要用到该游标id
                    scrollId = scrollResponce.getScrollId();
                    // 记录所有游标id
                    scrollIds.add(scrollId);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //清除游标id
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.scrollIds(scrollIds);
        try {
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("清除滚动查询游标id失败");
            e.printStackTrace();
        }
        // 4、处理响应结果
        System.out.println("滚动查询返回数据：" + scrollResponce);
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:06
     * @description: 根据索引名称和文档id查询文档是否存在于ES
     **/
    @Test
    void multiQueryById() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        MultiGetRequest request = new MultiGetRequest();
        // user是索引名称，1000和2000都是文档id
        request.add("user", "1000");
        request.add("user", "2000");
        // 3、发送请求到ES
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        // 4、处理响应结果
        List<String> existIdList = new ArrayList<>();
        for (MultiGetItemResponse itemResponse : response) {
            MultiGetResponse.Failure failure = itemResponse.getFailure();
            GetResponse getResponse = itemResponse.getResponse();
            if (failure == null) {
                boolean exists = getResponse.isExists();
                String id = getResponse.getId();
                if (exists) {
                    existIdList.add(id);
                }
            } else {
                failure.getFailure().printStackTrace();
            }
        }
        System.out.println("数据存在于ES的文档id：" + existIdList);
        // 5、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:06
     * @description: 打印集群名称和健康状况
     **/
    @Test
    void printClusterNameAndStatus() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、定义请求对象
        ClusterHealthRequest request = new ClusterHealthRequest();
        // 3、发送请求到ES
        ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        // 4、获取健康状况
        ClusterHealthStatus status = response.getStatus();
        // 5、打印集群名称
        System.out.println("集群名称：" + response.getClusterName());
        // 6、打印集群状态
        System.out.println("集群健康状态：" + status.name());
        // 7、关闭ES客户端对象
        client.close();
    }


    /*
     * @author: henyi
     * @date: 2022/7/19 11:06
     * @description: 打印索引信息
     **/
    @Test
    void printIndexInfo() throws Exception {
        // 1、创建ES客户端对象
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        // 2、发送请求到ES
        Response response = client.getLowLevelClient().performRequest(new Request("GET", "/_cat/indices"));
        // 3、数据处理
        HttpEntity entity = response.getEntity();
        String responseStr = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        // 4、数据分解
        String[] indexInfoArr = responseStr.split("\n");
        for (String indexInfo : indexInfoArr) {
            // 4.1、索引信息输出
            String[] infoArr = indexInfo.split("\\s+");
            String status = infoArr[0];
            String open = infoArr[1];
            String name = infoArr[2];
            String id = infoArr[3];
            String mainShardNum = infoArr[4];
            String viceShardNum = infoArr[5];
            String docNum = infoArr[6];
            String deletedDocNum = infoArr[7];
            String allShardSize = infoArr[8];
            String mainShardSize = infoArr[9];
            System.out.println("》》》》》》》》索引信息》》》》》》》》");
            System.out.println("名称：" + name);
            System.out.println("id：" + id);
            System.out.println("状态：" + status);
            System.out.println("是否开放：" + open);
            System.out.println("主分片数量：" + mainShardNum);
            System.out.println("副本分片数量：" + viceShardNum);
            System.out.println("Lucene文档数量：" + docNum);
            System.out.println("被删除文档数量：" + deletedDocNum);
            System.out.println("所有分片大小：" + allShardSize);
            System.out.println("主分片大小：" + mainShardSize);
        }
        // 6、关闭ES客户端对象
        client.close();
    }


}
