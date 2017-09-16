package org.xbib.adapter.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.xbib.jdbc.JdbcPipeline;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sanyu on 2017/9/9.
 */
@Service
public class JdbcPipelineService {

    private static Map<String, JdbcPipeline> map = new HashMap<>();

    public static Map<String, JdbcPipeline> getMap() {
        return map;
    }

    @Async
    public void run(String id){
        JdbcPipeline jdbcPipeline = new JdbcPipeline();
        getMap().put(id, jdbcPipeline);
        jdbcPipeline.run(id);
    }

    public void shutdown(String id) throws Exception {
        getMap().get(id).shutdown();
    }

}
