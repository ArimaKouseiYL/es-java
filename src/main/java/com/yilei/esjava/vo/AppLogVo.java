package com.zlst.uimp.cloud.ccapi.module.log_search.vo;

import lombok.Data;

import java.util.Date;

@Data
public class AppLogVo {
    /**
     * 指定日期的index
     */
    private Date indexDate;

    /**
     * 查询参数
     */
    private QueryBar queryBar;

    /**
     * 查询的数据所属时间区间
     */
    private Date start;
    private Date end;

    /**
     * 数据返回的起始量
     * from，size为5，那么es将返回第8、9、10、11和12项结果
     */
    private Integer from;

    /**
     * 数据返回量
     */
    private Integer size;

    /**
     * 是否按时间降序
     * 0 或不传 代表 不排序，
     * 1 代表正序，
     * 2 代表降序
     */
    private String sort;

}
