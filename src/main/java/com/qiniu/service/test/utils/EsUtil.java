package com.qiniu.service.test.utils;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

public class EsUtil {


//    private static TransportClient getClient(){
//        String className = "org.elasticsearch.client.transport.TransportClient";
//        try {
//            client = (TransportClient) Class.forName(className).newInstance();
//        } catch (InstantiationException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        return client;
//    }

    /**
     * 简单的查询
     * select * from {index} where {field}={Accept} limit {size};
     * @param index  查询的索引
     * @param type   查询的type,可以使用heda查看
     * @param field  查询的字段
     * @param Accept 查询的内容
     * @param size   查询结果的条数
     * @return SearchResponse的Json
     */
    public static List<String> queryByFilter_Accept(TransportClient client, String index, String type, String field, String Accept, int size) {

        SearchResponse response = client.prepareSearch(index)//设置要查询的索引(index)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes(type)//设置type, 这个在建立索引的时候同时设置了, 或者可以使用head工具查看
                .setQuery(QueryBuilders.matchQuery(field, Accept)) //在这里"message"是要查询的field,"Accept"是要查询的内容
                .setFrom(0)
                .setSize(size)
                .setExplain(true)
                .execute()
                .actionGet();
        List<String> docList = new ArrayList<String>();
        for (SearchHit hit : response.getHits()) {
            docList.add(hit.getSourceAsString());
        }
        client.close();
        return docList;
    }

    /**
     * 简单的查询
     * select * from {index} limit {size};
     * @param index 查询的索引
     * @param type  查询的type,可以使用heda查看
     * @param size  查询结果的条数
     * @return 结果list
     */
    public static List<String> queryByFilter(TransportClient client, String index, String type, int size) {

        SearchResponse response = client.prepareSearch(index)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes(type)
                .setFrom(0)
                .setSize(size)
                .setExplain(true)
                .execute()
                .actionGet();
        List<String> docList = new ArrayList<String>();
        for (SearchHit hit : response.getHits()) {
            docList.add(hit.getSourceAsString());
        }
        client.close();
        return docList;
    }

}
