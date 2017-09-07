package org.xbib.jdbc.input;

import org.elasticsearch.common.settings.Settings;

import java.io.InputStream;

/**
 * Created by sanyu on 2017/9/7.
 */
public class InputImpl implements Input{

    @Override
    public void init() {
        InputStream in_config_template = getClass().getResourceAsStream(ROOT_PATH + "/config_template.json");

        Settings settings = Settings.settingsBuilder()
                .loadFromStream("config_template", in_config_template)
                .build();
        System.out.println(settings.getAsMap());
    }

    @Override
    public void prepare() {

    }

    @Override
    public void execute() {

    }

}
