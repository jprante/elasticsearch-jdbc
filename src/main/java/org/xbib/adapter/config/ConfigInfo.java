package org.xbib.adapter.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;

/**
 * Created by sanyu on 2017/8/19.
 */
public class ConfigInfo {
    private String type = "jdbc";

    @SerializedName("jdbc") // map field to Json name
    @JsonProperty("jdbc") // map json name to field
    private JdbcConfigInfo jdbcConfigInfo;

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

}
