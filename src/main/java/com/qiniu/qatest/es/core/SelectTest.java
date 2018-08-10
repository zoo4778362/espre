package com.qiniu.qatest.es.core;

import com.qiniu.pandora.common.PandoraClient;
import com.qiniu.pandora.common.PandoraClientImpl;
import com.qiniu.pandora.common.QiniuException;
import com.qiniu.pandora.logdb.LogDBClient;
import com.qiniu.pandora.logdb.search.MultiSearchService;
import com.qiniu.pandora.logdb.search.PartialSearchService;
import com.qiniu.pandora.logdb.search.ScrollSearchService;
import com.qiniu.pandora.logdb.search.SearchService;
import com.qiniu.pandora.util.Auth;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.util.ArrayList;
import java.util.List;

public class SelectTest extends AbstractJavaSamplerClient {


    private String ak;
    private String sk;
    private String repoName;
    private Auth auth;
    private LogDBClient client;
    private String logDBHost;
    private String searchType;
    private String searchSql;
    private ScrollSearchService scrollSearchService;
    private MultiSearchService multiSearchService;
    private PartialSearchService partialSearchService;
    private SearchService searchService;
    private Boolean success;
    private String searchscrool;

    @Override
    public void setupTest(JavaSamplerContext context) {
        ak = context.getParameter("ak");
        sk = context.getParameter("sk");
        repoName = context.getParameter("reponame");
        logDBHost = context.getParameter("logdbhost");
        searchType = context.getParameter("searchtype");
        searchSql = context.getParameter("searchsql");
        searchscrool = context.getParameter("searchscrool");
        auth = Auth.create(ak, sk);
        client = new LogDBClient(new PandoraClientImpl(auth), logDBHost);
        success = true;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {

        super.teardownTest(context);
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        arguments.addArgument("ak", "七牛ak");
        arguments.addArgument("sk", "七牛sk");
        arguments.addArgument("reponame", "七牛logdb仓库名");
        arguments.addArgument("serachtype", "搜索类型");//看注释
        arguments.addArgument("logdbhost", "logdb路由地址");
        arguments.addArgument("searchsql", "查询语句");
        arguments.addArgument("searchscrool", "查询间隔");
        return super.getDefaultParameters();
    }

    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult sr = new SampleResult();
        try {
            //记入开始时间
            sr.sampleStart();
            qiniuQuery();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //记录结束时间
            sr.sampleEnd();
        }
        return sr;
    }

    private void qiniuQuery() {
        int serachtype = Integer.parseInt(this.searchType);
        switch (serachtype) {
            case 1:
                success = multiSearch();
            case 2:
                success = partialSearch();
            case 3:
                success = scrollSearch();
            case 4:
                success = search();
            default:
                break;
        }
    }


    //多个repo查询操作 注意操作,如果大面积超时直接报错
    private Boolean multiSearch() {
        List<MultiSearchService.SearchRequest> list = new ArrayList<MultiSearchService.SearchRequest>();
        multiSearchService = this.client.NewMultiSearchService();
        MultiSearchService.SearchRequest searchRequest = new MultiSearchService.SearchRequest();
        searchRequest.source = this.searchSql;
        list.add(searchRequest);
        try {
            multiSearchService.search(list);
        } catch (QiniuException e) {
            e.printStackTrace();
            success = false;
        }
        return success;
    }

    //用于拉取大规模数据，配合搜索日志查询接口
    private Boolean scrollSearch() {
        SearchService searchService = this.client.NewSearchService();
        ScrollSearchService scrollSearchService = this.client.NewScrollSearchService();
        SearchService.SearchRequest searchRequest = new SearchService.SearchRequest();
        searchRequest.scroll = this.searchscrool;
        searchRequest.query = this.searchSql;
        searchRequest.size = 7;
        scrollSearchService = this.client.NewScrollSearchService();
        try {
            SearchService.SearchResult result = searchService.search(repoName, searchRequest);
            String scrollId = result.scrollID;
            scrollSearchService.scroll(repoName, this.searchscrool, scrollId);
        } catch (QiniuException e) {
            e.printStackTrace();
            success = false;
        }
        return success;
    }

    //非永久存储的repo且具有时间字段的repo查询,对超大日志有具体优化
    private Boolean partialSearch() {
        partialSearchService = this.client.NewPartialSearchService();
        return success;
    }

    //最常用的查询数据
    private Boolean search() {
        return success;
    }
}



