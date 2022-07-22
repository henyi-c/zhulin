package com.zhulin.common.es.utils;


import com.zhulin.common.es.constant.ES_CompareEnum;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.Objects;

import static com.zhulin.common.es.constant.ES_Const.ELASTIC_SEARCH_KEY_WORD;

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
 *
 *
 * （*） 表示任意个，(0个或多个) ；
 * （?） 表示出现0次或多次 (有或者没有);
 * (+) 一次或多次(至少出现一次)；
 * QueryBuilders.wildcardQuery(key, "*" + value + "*") 和like%xxx%一致
 */
public class ES_HelpUtil {


    /**
     * eq精准匹对键值
     * 对于一些人名、地名、xx名 非Text长描述类型的加 .keyword进行关键字匹对
     *
     * @param key   键
     * @param value 值
     * @return QueryBuilder
     */
    public static QueryBuilder eq(String key, Object value) {
        return value instanceof String ? QueryBuilders.termQuery(key + ELASTIC_SEARCH_KEY_WORD, value) : QueryBuilders.termQuery(key, value);
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
    public static QueryBuilder like(String key, String value) {
        return QueryBuilders.wildcardQuery(key, "*" + value + "*");
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
        return range(key, startValue, true, endValue, true);
    }


    /**
     * 范围值查询/大小查询
     *
     * @param key        键
     * @param startValue 开始值
     * @param endValue   结束值
     * @return QueryBuilder
     */
    public static QueryBuilder range(String key, Object startValue, boolean needGtForStart, Object endValue, boolean needGtForEnd) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(key);
        if (Objects.nonNull(startValue)) {
            rangeQueryBuilder = needGtForStart ? rangeQueryBuilder.gte(startValue) : rangeQueryBuilder.gt(startValue);
        }
        if (Objects.nonNull(endValue)) {
            rangeQueryBuilder = needGtForEnd ? rangeQueryBuilder.lte(endValue) : rangeQueryBuilder.lt(endValue);
        }
        boolQueryBuilder.filter(rangeQueryBuilder);
        return boolQueryBuilder;
    }


    /**
     * 范围值查询/大小查询
     *
     * @param esCompareEnum 枚举
     * @param key           键
     * @param value         值
     * @return QueryBuilder
     */
    public static QueryBuilder compare(ES_CompareEnum esCompareEnum, String key, Object value) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(key);
        switch (esCompareEnum) {
            case gt:
                rangeQueryBuilder.to(value);
                break;
            case lt:
                rangeQueryBuilder.lt(value);
                break;
            case gte:
                rangeQueryBuilder.gte(value);
                break;
            case lte:
                rangeQueryBuilder.lte(value);
                break;
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
    public static QueryBuilder or(QueryBuilder... builders) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (QueryBuilder builder : builders) {
            boolQueryBuilder.should(builder);
        }
        return boolQueryBuilder;
    }


    /**
     * and条件处理
     *
     * @param builders QueryBuilder对象
     * @return QueryBuilder
     */
    public static QueryBuilder and(QueryBuilder... builders) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (QueryBuilder builder : builders) {
            boolQueryBuilder.must(builder);
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
