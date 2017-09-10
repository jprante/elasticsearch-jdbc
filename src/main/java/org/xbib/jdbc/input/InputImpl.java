package org.xbib.jdbc.input;

import org.elasticsearch.common.settings.Settings;

import java.io.InputStream;

/**
 * Created by sanyu on 2017/9/7.
 */
public class InputImpl implements Input{

    @Override
    public void init() {
        // use config_template.json should have the same structure as package, so use absolute path here
        InputStream in_config_template = getClass().getResourceAsStream(ROOT_PATH + "/config_template.json");

        // TODO: get all configs as stream
        // TODO: replace place_holder and merge configs
        Settings settings = Settings.settingsBuilder()
                .loadFromStream("config_template", in_config_template)
                .build();
        System.out.println(settings.getAsMap());
        System.out.println(settings.getAsStructuredMap().containsKey("jdbc/maxrows"));
    }

    @Override
    public void prepare() {

    }

    @Override
    public void execute() {

    }

}
