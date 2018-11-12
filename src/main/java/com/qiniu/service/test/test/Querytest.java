package com.qiniu.service.test.test;


import com.qiniu.service.test.core.ESSearchClent;
import com.qiniu.service.test.entity.EsClient;
import com.qiniu.service.test.utils.EsUtil;
import org.elasticsearch.client.transport.TransportClient;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;

public class Querytest {

    private static String host = "10.200.20.40";
    private static String port = "9200";
    private static String clusterName = "qiniues";
    private static EsClient esClient;
    private static TransportClient client;

    @BeforeTest
    public void before_Test(){
        esClient = new EsClient();
        esClient.setHost(host);
        esClient.setPort(port);
        esClient.setClusterName(clusterName);
        System.out.println(esClient.toString());
        client = ESSearchClent.getClient(esClient);
    }

    @Test
    public void test_1(){
        List<String> ls = EsUtil.queryByFilter(client,"app-1810769119-logkit_pro_internal-1540352893144974873-qiniues-0-2018.11.08","app",10);
        System.out.println(ls.toString());
    }
}
