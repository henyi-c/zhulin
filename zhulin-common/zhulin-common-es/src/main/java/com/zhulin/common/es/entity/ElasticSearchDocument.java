package com.zhulin.common.es.entity;


import lombok.Data;

/*
 * @author: henyi
 * @date: 2022/7/18 17:41
 * @description: 自定义表数据封装
 **/
@Data
public class ElasticSearchDocument<T> {

    /*
     * 主键
     **/
    private String id;

    /*
     * 数据
     **/
    private T data;
}
