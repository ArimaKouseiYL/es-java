package com.zlst.uimp.cloud.ccapi.module.log_search.api;

import com.zlst.uimp.cloud.ccapi.common.Assertion;
import com.zlst.uimp.cloud.ccapi.module.log_search.service.EsService;
import com.zlst.uimp.cloud.ccapi.module.log_search.vo.AppLogVo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;

@RestController
@RequestMapping("/api/esLog")
@Api(tags = {"esLog"}, description = "es中的日志搜索、下载")
@Slf4j
public class EsCtr {

    @Autowired
    private EsService esService;

    private final String DATE_FORMAT = "yyyy.MM.dd";
    private final String NONE_DATE_APP_LOG_NAME_SUFFIX = "app-log-*";
    private final String APP_LOG_NAME_SUFFIX = "app-log-";

    @GetMapping("/list")
    @ApiOperation(value = "指定条件，获取es中的日志")
    public Assertion searchLog(@RequestBody(required = false) AppLogVo appLogVo) {
        return new Assertion(esService.searchLog(appLogVo));
    }


    @GetMapping("/download")
    @ApiOperation(value = "下载，指定条件下获取的es日志")
    public Assertion downloadLog(@RequestBody(required = false) AppLogVo appLogVo, HttpServletResponse response) {
        SearchHit[] searchHits = esService.downloadLog(appLogVo);
        ServletOutputStream outSTr = null;
        BufferedOutputStream buff = null;
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        String indexName = appLogVo.getIndexDate() == null ? NONE_DATE_APP_LOG_NAME_SUFFIX : APP_LOG_NAME_SUFFIX + sdf.format(appLogVo.getIndexDate());
        response.setHeader("Content-Disposition", " ; filename=" + indexName + ".txt");
        response.setContentType("text/plain");
        try {
            outSTr = response.getOutputStream();
            buff = new BufferedOutputStream(outSTr);
            for (int i = 0; i < searchHits.length; i++) {
                buff.write(searchHits[i].getSourceAsMap().toString().getBytes());
            }
            buff.flush();
            buff.close();
        } catch (IOException e) {
            log.error("", e);
        } finally {
            try {
                buff.close();
                outSTr.close();
            } catch (Exception e) {
                log.error("", e);
            }

        }
        return new Assertion();
    }

}
