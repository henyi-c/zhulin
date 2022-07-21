package com.zhulin.common.es.utils;


import com.zhulin.common.es.constant.ES_CodeTypEnum;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.Objects;

/**
 * ES 条件封装
 * QueryBuilders.matchQuery
 * 输入i li，即使es没找到li那个词, 但找到i的词组，也算匹对成功，也就是任意有一个词匹对上都可以
 * <p>
 * QueryBuilders.matchPhraseQuery
 * 输入的关键字分词后按分词顺序查找 可以认为查找的词组必须包含输入的关键字
 * 但是关键字必须完整，如果输入i li，li不是一个完整的词，词组里找不到该分词，那么就依旧找不到
 * <p>
 * QueryBuilders.matchPhrasePrefixQuery
 * 和like的'xxxxx%'是类似的
 * <p>
 * QueryBuilders.termQuery 只能查一个完整的分词
 * <p>
 * <p>
 * QueryBuilders.fuzzyQuery("name.keyword", "小").fuzziness(Fuzziness.ONE)
 * .keyword代表做关键词，不分词
 * Fuzziness.ONE存在一个错误的字母，可以匹对单词
 */
public class ES_HelpUtil {


    /**
     * eq精准匹对键值
     *
     * @param key   键
     * @param value 值
     * @return QueryBuilder
     */
    public static QueryBuilder eq(String key, String value) {
        return QueryBuilders.termQuery(key, value);
    }


    /**
     * 匹对可能错误的值
     *
     * @param key   键
     * @param value 值
     * @return QueryBuilder
     */
    public static QueryBuilder eqIgnoreFail(String key, String value, Fuzziness fuzziness) {
        return QueryBuilders.fuzzyQuery(key, value).fuzziness(fuzziness);
    }

    /**
     * like模糊匹对键值
     *
     * @param key   键
     * @param value 值
     * @return QueryBuilder
     */
    public static QueryBuilder like(String key, String value, ES_CodeTypEnum esCodeTypEnum) {
        return esCodeTypEnum.equals(ES_CodeTypEnum.CN) ? QueryBuilders.matchPhraseQuery(key, value) : QueryBuilders.matchPhrasePrefixQuery(key, value);
    }


    /**
     * 范围值查询/大小查询
     *
     * @param key        键
     * @param startValue 开始值
     * @param endValue   结束值
     * @return QueryBuilder
     */
    public static QueryBuilder range(String key, Object startValue, Object endValue) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(key);
        if (Objects.nonNull(startValue)) {
            rangeQueryBuilder.gte(startValue);
        }
        if (Objects.nonNull(endValue)) {
            rangeQueryBuilder.lte(endValue);
        }
        boolQueryBuilder.filter(rangeQueryBuilder);
        return boolQueryBuilder;
    }


    /**
     * or条件处理
     *
     * @param builders QueryBuilder对象
     * @return QueryBuilder
     */
    public static QueryBuilder or(QueryBuilder[] builders) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (QueryBuilder queryBuilder : builders) {
            boolQueryBuilder.should(queryBuilder);
        }
        return boolQueryBuilder;
    }


    /**
     * and条件处理
     *
     * @param builders QueryBuilder对象
     * @return QueryBuilder
     */
    public static QueryBuilder and(QueryBuilder[] builders) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (QueryBuilder queryBuilder : builders) {
            boolQueryBuilder.must(queryBuilder);
        }
        return boolQueryBuilder;
    }


    /**
     * 设置关键字高亮
     *
     * @param fieldName 字段名称
     * @param builder   构造器
     * @param colorCode ex: red/#FF0033都可以
     * @return SearchSourceBuilder
     */
    public static SearchSourceBuilder highlight(String fieldName, SearchSourceBuilder builder, String colorCode) {
        // 构建高亮查询对象
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        // 前置标签
        highlightBuilder.preTags("<b style='color:" + colorCode + "'>");
        // 后置标签
        highlightBuilder.postTags("</b>");
        // 添加高亮的属性名称
        highlightBuilder.field(fieldName);
        return builder.highlighter(highlightBuilder);
    }


}
