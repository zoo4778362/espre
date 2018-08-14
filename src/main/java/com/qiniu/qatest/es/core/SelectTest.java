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
import java.util.Random;

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
    private String searchScrool;
    private String searchParam;
    private int needChangeSql;
    private Boolean changeSql;

    @Override
    public void setupTest(JavaSamplerContext context) {
        ak = context.getParameter("ak");
        sk = context.getParameter("sk");
        repoName = context.getParameter("reponame");
        logDBHost = context.getParameter("logdbhost");
        searchType = context.getParameter("searchtype");
        searchSql = context.getParameter("searchsql");
        searchScrool = context.getParameter("searchscrool");
        searchParam = context.getParameter("searchparam");
        auth = Auth.create(ak, sk);
        client = new LogDBClient(new PandoraClientImpl(auth), logDBHost);
        needChangeSql = context.getIntParameter("needchangesql");
        success = true;
        if(needChangeSql > 0){
            changeSql = true;
        }else {
            changeSql = false;
        }
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
        arguments.addArgument("searchparam","用,区分各个参数");
        arguments.addArgument("needchangesql","0：不需要 1：需要");
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
        searchRequest.source = replaceValue();
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
        searchRequest.scroll = this.searchScrool;
        searchRequest.query = replaceValue();
        searchRequest.size = 7;
        scrollSearchService = this.client.NewScrollSearchService();
        try {
            SearchService.SearchResult result = searchService.search(repoName, searchRequest);
            String scrollId = result.scrollID;
            scrollSearchService.scroll(repoName, this.searchScrool, scrollId);
        } catch (QiniuException e) {
            e.printStackTrace();
            success = false;
        }
        return success;
    }

    //非永久存储的repo且具有时间字段的repo查询,对超大日志有具体优化
    private Boolean partialSearch() {
        partialSearchService = this.client.NewPartialSearchService();
        PartialSearchService.SearchRequest request = new PartialSearchService.SearchRequest();
        //参数用,分隔，严格按照定义
        String[] params = this.searchParam.split(",");
        request.queryString = replaceValue();
        request.size = Integer.parseInt(params[0]);
        request.sort = "timestamp";
        request.startTime = Long.parseLong(params[1]);
        request.endTime = Long.parseLong(params[2]);
        try {
            PartialSearchService.SearchResult result = partialSearchService.search(repoName,request);
        } catch (QiniuException e) {
            e.printStackTrace();
            success = false;
        }
        return success;
    }

    //最常用的查询数据
    private Boolean search() {
        searchService = this.client.NewSearchService();
        SearchService.SearchRequest request = new SearchService.SearchRequest();
        String[] paranms = this.searchParam.split(",");
        request.size = Integer.parseInt(paranms[0]);
        request.query = replaceValue();
        try {
            SearchService.SearchResult result = searchService.search(repoName,request);
        } catch (QiniuException e) {
            e.printStackTrace();
            success = false;
        }
        return success;
    }

    //固定日志格式，对指定字段进行随机匹配,固定随机第一个字段
    //success:0 OR success:7166437 OR success:7173351 OR success:7165826 OR success:7167148 默认这样的
    private String replaceValue(){
        if(!this.changeSql){
            return this.searchSql;
        }
        String splitstr = "";
        String sql = this.searchSql.trim(); //先去掉空格
        if(sql.contains("OR")){
            splitstr = "OR";
        }else {
            splitstr = "AND";
        }
        String[] ss = sql.split(splitstr);
        if (ss.length < 0){
            return this.searchSql;
        }
        String value = ss[0].split(":")[1];
        String value_after = "";
        //四分之一的概率不需要拼接
        if(new Random().nextInt(100) > 25){
            String time = String.valueOf(System.currentTimeMillis());
            //取时间戳的最后9位进行拼接
            value_after = value + time.substring(time.length()-9,time.length());
            //替换原有的
            this.searchSql.replace(value,value_after);
        }
        return this.searchSql;
    }
}



