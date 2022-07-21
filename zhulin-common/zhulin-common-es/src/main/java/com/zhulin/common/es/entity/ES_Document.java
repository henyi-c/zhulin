package com.zhulin.common.es.entity;


import lombok.Data;

/*
 * @author: henyi
 * @date: 2022/7/18 17:41
 * @description: 自定义表数据封装
 **/
@Data
public class ES_Document<T> {


    /*
     * 表id
     **/
    private String index;

    /*
     * 数据id
     **/
    private String documentId;

    /*
     * 数据
     **/
    private T data;

    public ES_Document() {
    }

    public ES_Document(String index, T sourceAsString) {
        this.index = index;
        this.data = sourceAsString;
    }
    public ES_Document(String index, String documentId, T sourceAsString) {
        this.index = index;
        this.documentId = documentId;
        this.data = sourceAsString;
    }
}
