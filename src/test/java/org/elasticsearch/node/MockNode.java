
package org.elasticsearch.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class MockNode extends Node {

    public MockNode(Settings settings) {
        this(settings, Collections.<Class<? extends Plugin>>emptyList());
    }

    public MockNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
        super(InternalSettingsPreparer.prepareEnvironment(settings, null), Version.CURRENT, classpathPlugins);
    }

    public MockNode(Settings settings, Class<? extends Plugin> classpathPlugin) {
        this(settings, list(classpathPlugin));
    }

    private static Collection<Class<? extends Plugin>> list(Class<? extends Plugin> classpathPlugin) {
        Collection<Class<? extends Plugin>> list = new ArrayList<>();
        list.add(classpathPlugin);
        return list;
    }

}
