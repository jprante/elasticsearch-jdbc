
package org.elasticsearch.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;

public class MockNode extends Node {

    public MockNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
        super(settings, Version.CURRENT, classpathPlugins);
    }

}
