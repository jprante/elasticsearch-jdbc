package org.xbib.adapter.config;

import com.google.gson.annotations.SerializedName;

/**
 * Created by sanyu on 2017/8/19.
 */
public class JdbcConfigInfo {


    private String url = "jdbc:mysql://localhost:3306/sbes";
    private String user = "root";
    private String password = "";
    private String sql =  "select * from s_movie";
    private Boolean treat_binary_as_string = true;
    @SerializedName("elasticsearch")
    private ElasticsearchConfigInfo elasticsearchConfigInfo;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Boolean getTreat_binary_as_string() {
        return treat_binary_as_string;
    }

    public void setTreat_binary_as_string(Boolean treat_binary_as_string) {
        this.treat_binary_as_string = treat_binary_as_string;
    }

    public ElasticsearchConfigInfo getElasticsearchConfigInfo() {
        return elasticsearchConfigInfo;
    }

    public void setElasticsearchConfigInfo(ElasticsearchConfigInfo elasticsearchConfigInfo) {
        this.elasticsearchConfigInfo = elasticsearchConfigInfo;
    }

}
