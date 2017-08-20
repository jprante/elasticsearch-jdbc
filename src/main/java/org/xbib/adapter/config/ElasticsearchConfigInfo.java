package org.xbib.adapter.config;

/**
 * Created by sanyu on 2017/8/19.
 */
public class ElasticsearchConfigInfo {

    private String cluster = "elasticsearch";
    private String host = "localhost";
    private Integer port = 9300;

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

}
