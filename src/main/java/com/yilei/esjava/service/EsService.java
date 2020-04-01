package com.zlst.uimp.cloud.ccapi.module.log_search.service;

import com.zlst.uimp.cloud.ccapi.exception.UimpException;
import com.zlst.uimp.cloud.ccapi.module.log_search.vo.AppLogVo;
import com.zlst.uimp.cloud.ccapi.module.log_search.vo.QueryBar;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Auther: yilei
 * @Date: 2020/3/31
 * @Description:
 */
@Service
@Slf4j
public class EsService {
    private final String TYPE = "_doc";
    private final String DATE_FORMAT = "yyyy.MM.dd";
    private final String NONE_DATE_APP_LOG_NAME_SUFFIX = "app-log-*";
    private final String APP_LOG_NAME_SUFFIX = "app-log-";
    private final String TIMESTAMP = "@timestamp";
    private final String GREATER_THAN = ">";
    private final String LESS_THAN = "<";
    private final String GREATER_THAN_EQUAL = ">=";
    private final String LESS_THAN_EQUAL = "<=";
    private final Integer DEFAULT_PAGE_FROM = 20;
    private final Integer DEFAULT_PAGE_SIZE = 10;
    private final String KEYWORD = ".keyword";

    @Autowired
    private TransportClient client;

    /**
     * 查询日志
     */
    public List searchLog(AppLogVo appLogVo) {
        SearchHit[] arrayHit = getSearchHits(appLogVo);
        List resultList = new ArrayList();
        if (null != arrayHit && arrayHit.length > 0) {
            for (int i = 0; i < arrayHit.length; i++) {
                resultList.add(arrayHit[i].getSourceAsMap());
            }
        }
        return resultList;
    }

    /**
     * 查询日志source
     */
    public SearchHit[] downloadLog(AppLogVo appLogVo) {
        SearchHit[] arrayHit = getSearchHits(appLogVo);
        return arrayHit;
    }

    private SearchHit[] getSearchHits(AppLogVo appLogVo) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 查询条件
        if (null != appLogVo && null != appLogVo.getQueryBar()) {
            QueryBar queryBar = appLogVo.getQueryBar();
            Map mustQuery = queryBar.getMustQuery();
            Map shouldQuery = queryBar.getShouldQuery();
            Map notQuery = queryBar.getNotQuery();

            if (!CollectionUtils.isEmpty(mustQuery)) {
                Iterator it = mustQuery.keySet().iterator();
                while (it.hasNext()) {
                    String key = it.next().toString();
                    String value = mustQuery.get(key).toString();
                    if (value.contains("*")) { // 模糊匹配
                        WildcardQueryBuilder termQueryBuilder = QueryBuilders.wildcardQuery(key + KEYWORD, value);
                        boolQuery.must(termQueryBuilder);
                    } else { // 精准匹配
                        if (!value.contains(GREATER_THAN) && !value.contains(LESS_THAN) && !value.contains(GREATER_THAN_EQUAL) && !value.contains(LESS_THAN_EQUAL)) {
                            //MatchQueryBuilder termQueryBuilder = QueryBuilders.matchQuery(key, value).operator(Operator.AND);
                            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(key + KEYWORD, value);
                            boolQuery.must(termQueryBuilder);
                        } else { // 有  > 、 >= 、 < 、 <= 等操作,范围匹配
                            RangeQueryBuilder rangeQueryBuilder = getRangeQueryBuilder(key + KEYWORD, value);
                            boolQuery.must(rangeQueryBuilder);
                        }
                    }
                }
            }

            if (!CollectionUtils.isEmpty(shouldQuery)) {
                Iterator it = shouldQuery.keySet().iterator();
                while (it.hasNext()) {
                    String key = it.next().toString();
                    String value = shouldQuery.get(key).toString();
                    if (value.contains("*")) { // 模糊匹配
                        WildcardQueryBuilder termQueryBuilder = QueryBuilders.wildcardQuery(key + KEYWORD, value);
                        boolQuery.should(termQueryBuilder);
                    } else { // 精准匹配
                        if (!value.contains(GREATER_THAN) && !value.contains(LESS_THAN) && !value.contains(GREATER_THAN_EQUAL) && !value.contains(LESS_THAN_EQUAL)) {
                            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(key + KEYWORD, value);
                            boolQuery.should(termQueryBuilder);
                        } else {
                            RangeQueryBuilder rangeQueryBuilder = getRangeQueryBuilder(key + KEYWORD, value);
                            boolQuery.should(rangeQueryBuilder);
                        }
                    }
                }
            }

            if (!CollectionUtils.isEmpty(notQuery)) {
                Iterator it = notQuery.keySet().iterator();
                while (it.hasNext()) {
                    String key = it.next().toString();
                    String value = notQuery.get(key).toString();
                    if (value.contains("*")) { // 模糊匹配
                        WildcardQueryBuilder termQueryBuilder = QueryBuilders.wildcardQuery(key + KEYWORD, value);
                        boolQuery.mustNot(termQueryBuilder);
                    } else { // 精准匹配
                        if (!value.contains(GREATER_THAN) && !value.contains(LESS_THAN) && !value.contains(GREATER_THAN_EQUAL) && !value.contains(LESS_THAN_EQUAL)) {
                            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(key + KEYWORD, value);
                            boolQuery.mustNot(termQueryBuilder);
                        } else {
                            RangeQueryBuilder rangeQueryBuilder = getRangeQueryBuilder(key + KEYWORD, value);
                            boolQuery.mustNot(rangeQueryBuilder);
                        }
                    }
                }
            }
        }

        // 时间过滤
        if (null != appLogVo.getStart() && null != appLogVo.getEnd()) {
            boolQuery.filter(QueryBuilders.rangeQuery(TIMESTAMP).gte(appLogVo.getStart().getTime()).lte(appLogVo.getEnd().getTime()));
        }
        // index
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        String indexName = appLogVo.getIndexDate() == null ? NONE_DATE_APP_LOG_NAME_SUFFIX : APP_LOG_NAME_SUFFIX + sdf.format(appLogVo.getIndexDate());

        Integer from = appLogVo.getFrom() == null ? DEFAULT_PAGE_FROM : appLogVo.getFrom();
        Integer size = appLogVo.getSize() == null ? DEFAULT_PAGE_SIZE : appLogVo.getSize();

        try {
            //数据分页 SearchSourceBuilder对象的from方法设置起始位置，size方式设置显示的个数，可以实现数据分页。
            SearchRequestBuilder srb = client.prepareSearch(indexName)
                    .setTypes(TYPE)
                    .setQuery(boolQuery)
                    .setFrom(from)
                    .setSize(size);
            SearchResponse searchResponse = null;
            if (appLogVo.getSort() == null || "0".equals(appLogVo.getSort())) {
                searchResponse = srb.execute().actionGet();
            } else if ("1".equals(appLogVo.getSort())) {
                searchResponse = srb.addSort(TIMESTAMP, SortOrder.ASC)
                        .execute().actionGet();
            } else if ("2".equals(appLogVo.getSort())) {
                searchResponse = srb.addSort(TIMESTAMP, SortOrder.DESC)
                        .execute().actionGet();
            }
            SearchHits hits = searchResponse.getHits();
            return hits.getHits();
        } catch (Exception e) {
            throw new UimpException("查询es数据出错！");
        }
    }

    private RangeQueryBuilder getRangeQueryBuilder(String key, String value) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(key);
        try { // eg:  > 20  , < 20 , >= 20 ,<= 20
            String[] va = new String[2];
            if (value.contains(GREATER_THAN)) {
                va = value.split(GREATER_THAN);
                rangeQueryBuilder.gt(va[1]);
            } else if (value.contains(LESS_THAN)) {
                va = value.split(LESS_THAN);
                rangeQueryBuilder.lt(va[1]);
            } else if (value.contains(GREATER_THAN_EQUAL)) {
                va = value.split(GREATER_THAN_EQUAL);
                rangeQueryBuilder.gte(va[1]);
            } else if (value.contains(LESS_THAN_EQUAL)) {
                va = value.split(LESS_THAN_EQUAL);
                rangeQueryBuilder.lte(va[1]);
            }
        } catch (Exception e) {
            throw new UimpException("请求参数表达式，格式错误！");
        }
        return rangeQueryBuilder;
    }

}
