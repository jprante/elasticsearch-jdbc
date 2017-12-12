package org.xbib.adapter;

import com.google.gson.Gson;
import org.elasticsearch.common.settings.Settings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.xbib.adapter.config.ConfigInfo;
import org.xbib.adapter.config.ConfigedItem;
import org.xbib.adapter.service.JdbcPipelineService;
import org.xbib.jdbc.JdbcPipeline;

import java.util.*;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 * Created by sanyu on 2017/8/20.
 */
@RestController
@RequestMapping("api")
public class JdbcAdpaterController {

    @Autowired
    private JdbcPipelineService jdbcPipelineService;

    @GetMapping("run/{id}")
    public ResponseEntity<String> run(@PathVariable String id) {
        jdbcPipelineService.run(id);
        return ResponseEntity.ok("running..." + id);
    }

    @GetMapping("shutdown/{id}")
    public ResponseEntity<String> shutdown(@PathVariable String id) throws Exception {
        jdbcPipelineService.shutdown(id);
        return ResponseEntity.ok("shutdown..." + id);
    }

    @PostMapping("config")
    public ResponseEntity saveConfig(@RequestBody ConfigInfo configInfo) {
        String index = configInfo.getJdbcConfigInfo().getIndex();

        if (null == SavedSettings.getSettings(index)) {
            Gson gson = new Gson();
            String json = gson.toJson(configInfo);
            Settings settings = settingsBuilder().loadFromSource(json).build();
            SavedSettings.addSettings(index, settings);
            return new ResponseEntity(HttpStatus.OK);
        }else{
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("list")
    public ResponseEntity<List<ConfigedItem>> getImporters() {
        List<ConfigedItem> configedItems = new ArrayList<>();

        Iterator iterator = SavedSettings.getSettingsMap().keySet().iterator();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();

            ConfigedItem configedItem = new ConfigedItem();
            configedItem.setName(key);
            configedItem.setIsActive(jdbcPipelineService.getMap().get(key) == null ? false : jdbcPipelineService.getMap().get(key).isActive());
            configedItems.add(configedItem);
        }

        return ResponseEntity.ok(configedItems);
    }

}
