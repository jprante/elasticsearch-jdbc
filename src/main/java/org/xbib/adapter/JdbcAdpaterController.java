package org.xbib.adapter;

import com.google.gson.Gson;
import org.elasticsearch.common.settings.Settings;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.xbib.adapter.config.ConfigInfo;
import org.xbib.adapter.config.ConfigedItem;
import org.xbib.tools.JDBCImporter;

import java.util.*;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 * Created by sanyu on 2017/8/20.
 */
@RestController
@RequestMapping("api")
public class JdbcAdpaterController {

    Map<String, JDBCImporter> map = new HashMap<>();

    @GetMapping("run/{id}")
    public ResponseEntity<String> run(@PathVariable String id) {
        JDBCImporter jdbcImporter = map.get(id);
        jdbcImporter.run();
        return ResponseEntity.ok("running..." + id);
    }

    @GetMapping("shutdown/{id}")
    public ResponseEntity<String> shutdown(@PathVariable String id) {
        JDBCImporter jdbcImporter = map.get(id);
        jdbcImporter.run();
        return ResponseEntity.ok("shutdown..." + id);
    }

    @PostMapping("config")
    public ResponseEntity<ConfigInfo> saveConfig(@RequestBody ConfigInfo configInfo) {
        String index = configInfo.getJdbcConfigInfo().getIndex();
        if (!map.containsKey(index)) {

            Gson gson = new Gson();
            String json = gson.toJson(configInfo);
            Settings settings = settingsBuilder().loadFromSource(json).build();

            JDBCImporter jdbcImporter = new JDBCImporter();
            jdbcImporter.setSettings(settings);
            map.put("importer-" + index, jdbcImporter);
        }
        return ResponseEntity.ok(configInfo);

    }

    @GetMapping("list")
    public ResponseEntity<List<ConfigedItem>> getImporters() {
        List<ConfigedItem> configedItems = new ArrayList<>();

        Iterator iterator = map.keySet().iterator();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();

            ConfigedItem configedItem = new ConfigedItem();
            configedItem.setName(key);
            configedItem.setIsActive(map.get(key).isActive());
            configedItems.add(configedItem);
        }

        return ResponseEntity.ok(configedItems);
    }

}
