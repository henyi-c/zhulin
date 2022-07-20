package com.zhulin.common.es.service;


import com.zhulin.common.es.entity.ES_Document;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface CommonSearchService {


    /**
     * 创建 ES 索引
     *
     * @param index 索引名
     * @return 返回 true，表示创建成功
     */
    boolean addIndex(String index) throws Exception;

    /**
     * 创建 ES 索引
     *
     * @param index          索引名
     * @param aliasesJsonStr 索引别名 {"user.aliases":{}}
     * @param mappingJsonStr 文档属性 {"properties":{"name":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"sex":{"type":"keyword"},"age":{"type":"integer"}}}
     * @param settingJsonStr 设置属性 {"index":{"number_of_shards":"9","number_of_replicas":"2"}}
     * @return 返回 true，表示创建成功
     */
    boolean addIndex(String index, String aliasesJsonStr, String mappingJsonStr, String settingJsonStr) throws Exception;


    /**
     * 判断索引是否存在
     *
     * @param index 索引
     * @return 返回 true，表示存在
     */
    boolean isExistIndex(String index) throws IOException;


    /**
     * 获取索引信息
     *
     * @param index 索引
     * @return aliases/mapping/setting的信息
     */
    Map<String, Object> getIndexInfo(String index) throws Exception;


    /**
     * 删除索引
     *
     * @param index 索引
     * @return 返回 true，表示删除成功
     */
    boolean deleteIndex(String index) throws Exception;


    /**
     * 添加数据到索引中
     *
     * @param ES_Document 封装数据对象
     * @return CREATED/UPDATED
     */
    String insertDataToIndex(ES_Document<?> ES_Document) throws Exception;


    /**
     * 局部更新索引中的数据
     *
     * @param ES_Document 封装数据对象
     * @return NOOP:无需修改/UPDATED:已更新
     */
    String updateDataToIndex(ES_Document<?> ES_Document) throws Exception;


    /**
     * 根据文档id查询索引中的数据
     *
     * @param index      索引
     * @param documentId 数据id
     * @return 返回通用封装的 ES_Document 对象  data默认为json字符串
     */
    ES_Document<?> getDataById(String index, String documentId) throws Exception;


    /**
     * 根据文档id查询索引中的数据
     *
     * @param index      索引
     * @param documentId 数据id
     * @param clz        ES_Document 对象的data类型
     * @return 返回通用封装的 ES_Document 对象
     */
    ES_Document<?> getDataById(String index, String documentId, Class<?> clz) throws Exception;


    /**
     * 根据条件查询数据
     *
     * @param index        索引
     * @param queryBuilder 查询构造器 QueryBuilders.matchAllQuery()
     * @param clz          ES_Document 对象的data类型
     * @return ES_Document列表对象
     */
    List<ES_Document<?>> queryData(String index, QueryBuilder queryBuilder, Class<?> clz) throws Exception;


    /**
     * 根据条件查询数据
     *
     * @param index        索引
     * @param queryBuilder 查询构造器 QueryBuilders.matchAllQuery()
     * @return Map列表对象
     */
    List<Map<String,Object>> queryData(String index, QueryBuilder queryBuilder) throws Exception;


    /**
     * 根据文档id删除索引中的数据
     *
     * @param index      索引
     * @param documentId 数据id
     * @return NOT_FOUND:没找到/DELETED:删除成功
     */
    String deleteDataById(String index, String documentId) throws Exception;


    /**
     * 返回删除失败的结果
     *
     * @param index        索引
     * @param queryBuilder 查询构造器
     * @return 返回删除失败的列表数据
     */
    List<BulkItemResponse.Failure> deleteDataByQuery(String index, QueryBuilder queryBuilder) throws IOException;


    /**
     * 批量插入数据
     *
     * @param ES_DocumentList 封装数据类集合
     * @return Map集合：batchInsertFlag:批量插入是否失败/successDocumentId:插入成功的文档id/failDocumentId:插入失败的文档id
     */
    Map<String, Object> batchInsertDataToIndex(List<ES_Document<?>> ES_DocumentList) throws Exception;


    /**
     * 批量删除数据
     *
     * @param ES_DocumentList 封装数据类集合
     * @return Map集合：batchDeleteFlag:批量插入是否失败/successDocumentId:插入成功的文档id/failDocumentId:插入失败的文档id
     */
    Map<String, Object> batchDeleteDataFromIndex(List<ES_Document<?>> ES_DocumentList) throws Exception;


}
