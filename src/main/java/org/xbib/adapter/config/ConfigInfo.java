package org.xbib.adapter.config;

import com.google.gson.annotations.SerializedName;

/**
 * Created by sanyu on 2017/8/19.
 */
public class ConfigInfo {
    private String type = "jdbc";
    @SerializedName("jdbc")
    private JdbcConfigInfo jdbcConfigInfo;
    private String index = "s_movie";

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public JdbcConfigInfo getJdbcConfigInfo() {
        return jdbcConfigInfo;
    }

    public void setJdbcConfigInfo(JdbcConfigInfo jdbcConfigInfo) {
        this.jdbcConfigInfo = jdbcConfigInfo;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }
}
