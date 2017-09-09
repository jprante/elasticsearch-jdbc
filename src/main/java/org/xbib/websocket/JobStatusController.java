package org.xbib.websocket;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.xbib.adapter.JdbcAdpaterController;
import org.xbib.adapter.SavedSettings;
import org.xbib.adapter.config.ConfigedItem;
import org.xbib.adapter.service.JdbcPipelineService;
import org.xbib.websocket.dto.JobStatus;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sanyu on 2017/9/9.
 */
@Controller
public class JobStatusController {

    @Autowired
    private JdbcPipelineService jdbcPipelineService;

    @MessageMapping("/topic/job/check")
    @SendTo("/topic/job/status")
    public JobStatus trackJobStatus() throws Exception {
        JobStatus jobStatus = new JobStatus();
        jobStatus.setName("test");
        jobStatus.setStatusCode(200L);

        List<ConfigedItem> configedItems = new ArrayList<>();

        Iterator iterator = SavedSettings.getSettingsMap().keySet().iterator();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();

            ConfigedItem configedItem = new ConfigedItem();
            configedItem.setName(key);
            configedItem.setIsActive(jdbcPipelineService.getMap().get(key) == null ? false : jdbcPipelineService.getMap().get(key).isActive());
            configedItems.add(configedItem);
        }

        jobStatus.setConfigedItems(configedItems);

        return jobStatus;
    }
}
