package com.qiniu.service.test.core;

import com.qiniu.service.test.entity.EsClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESSearchClent {

    private static TransportClient client;

    public static TransportClient getClient(EsClient esClient) {

        //如果已经存在了，直接返回
        if(client != null){
            return client;
        }

        Settings settings =Settings.builder()
                .put("cluster.name",esClient.getClusterName())
                .put("client.transport.sniff",true)//防止单点的时候出错
                .build();
        try {
            client = new PreBuiltTransportClient(settings).addTransportAddress(
                    new TransportAddress(InetAddress.getByName(esClient.getHost()),Integer.parseInt(esClient.getPort())));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

}
