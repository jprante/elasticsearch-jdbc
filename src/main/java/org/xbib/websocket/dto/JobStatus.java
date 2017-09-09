package org.xbib.websocket.dto;

import org.xbib.adapter.config.ConfigedItem;

import java.util.List;

/**
 * Created by sanyu on 2017/9/9.
 */
public class JobStatus {

    private String name;
    private Long statusCode;
    private List<ConfigedItem> configedItems;

    public List<ConfigedItem> getConfigedItems() {
        return configedItems;
    }

    public void setConfigedItems(List<ConfigedItem> configedItems) {
        this.configedItems = configedItems;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(Long statusCode) {
        this.statusCode = statusCode;
    }
}
