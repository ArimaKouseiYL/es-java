package com.zlst.uimp.cloud.ccapi.module.log_search.vo;

import lombok.Data;

import java.util.Map;

/**
 * @Auther: yilei
 * @Date: 2020/3/31
 * @Description:
 */
@Data
public class QueryBar {
    /**
     * 必须匹配的，如果must不为空，那么不需要设置should，must相当于&&操作
     */
    private Map<String, Object> mustQuery;
    /**
     * should相当于 || 操作
     */
    private Map<String, Object> shouldQuery;


    private Map<String, Object> notQuery;
}
