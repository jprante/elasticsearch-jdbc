package org.xbib.adapter;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.xbib.adapter.config.ConfigInfo;

/**
 * Created by sanyu on 2017/8/20.
 */
@RestController
@RequestMapping("api")
public class JdbcAdpaterController {

    @GetMapping("run")
    public ResponseEntity<String> run(){

        return ResponseEntity.ok("running...");
    }

    @GetMapping("shutdown")
    public ResponseEntity<String> shutdown(){
        return ResponseEntity.ok("shutdown...");
    }

    @PostMapping("config")
    public ResponseEntity<ConfigInfo> saveConfig(@RequestBody ConfigInfo configInfo){
        return ResponseEntity.ok(configInfo);
    }

}
