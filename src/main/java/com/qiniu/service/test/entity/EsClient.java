package com.qiniu.service.test.entity;

import lombok.Data;

@Data
public class EsClient {

    public String clusterName;//集群名字
    public String host;
    public String port;

}
