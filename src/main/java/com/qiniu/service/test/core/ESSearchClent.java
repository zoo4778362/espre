package com.qiniu.service.test.core;

import com.qiniu.service.test.entity.EsClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ESSearchClent {

    private static TransportClient client;

    public static synchronized TransportClient getClient(EsClient esClient) {

        //如果已经存在了，直接返回
        if(client != null){
            return client;
        }

        Settings settings =Settings.builder()
                .put("cluster.name",esClient.getClusterName())
                .put("client.transport.sniff",true)
                .build();
        try {
            TransportAddress ta1 = new InetSocketTransportAddress(InetAddress.getByName("cs20"), 9200);
            TransportAddress ta2 = new InetSocketTransportAddress(InetAddress.getByName("cs19"), 9200);
            TransportAddress ta3 = new InetSocketTransportAddress(InetAddress.getByName("cs19"), 9201);
            TransportAddress ta4 = new InetSocketTransportAddress(InetAddress.getByName("cs21"), 9200);
            client = TransportClient.builder().settings(settings).build().addTransportAddresses(ta1,ta2,ta3,ta4);
//            client = TransportClient.builder().settings(settings).build().addTransportAddress(
//                    new InetSocketTransportAddress(InetAddress.getByName(esClient.getHost()), Integer.parseInt(esClient.getPort())));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

}
