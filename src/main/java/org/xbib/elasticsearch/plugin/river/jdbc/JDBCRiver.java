package org.xbib.elasticsearch.plugin.river.jdbc;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.xbib.elasticsearch.action.river.execute.RunnableRiver;
import org.xbib.elasticsearch.action.river.state.RiverState;
import org.xbib.elasticsearch.action.river.state.StatefulRiver;
import org.xbib.elasticsearch.plugin.feeder.Feeder;
import org.xbib.elasticsearch.plugin.feeder.jdbc.JDBCFeeder;
import org.xbib.elasticsearch.plugin.jdbc.RiverServiceLoader;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * JDBC river
 */
public class JDBCRiver extends AbstractRiverComponent implements RunnableRiver, StatefulRiver {

    private final ESLogger logger = ESLoggerFactory.getLogger(JDBCRiver.class.getName());

    private final Client client;

    private final Feeder feeder;

    private volatile Thread riverThread;

    private volatile boolean closed;

    @Inject
    @SuppressWarnings({"unchecked"})
    public JDBCRiver(RiverName riverName, RiverSettings riverSettings, Client client) {
        super(riverName, riverSettings);
        if (!riverSettings.settings().containsKey("jdbc")) {
            throw new IllegalArgumentException("no 'jdbc' settings in river settings?");
        }
        this.client = client;
        this.feeder = createFeeder(riverName.getType(), riverName.getName(), riverSettings);
    }

    private Feeder createFeeder(String riverType, String riverName, RiverSettings riverSettings) {
        JDBCFeeder feeder = null;
        try {
            Map<String, Object> spec = (Map<String, Object>) riverSettings.settings().get("jdbc");
            Map<String, String> loadedSettings = new JsonSettingsLoader().load(jsonBuilder().map(spec).string());
            Settings mySettings = settingsBuilder().put(loadedSettings).build();
            String strategy = XContentMapValues.nodeStringValue(spec.get("strategy"), "simple");
            RiverFlow riverFlow = RiverServiceLoader.findRiverFlow(strategy);
            logger.debug("found river flow class {} for strategy {}", riverFlow.getClass().getName(), strategy);
            feeder = riverFlow.getFeeder();
            logger.debug("spec = {} settings = {}", spec, mySettings.getAsMap());
            feeder.setName(riverName)
                    .setType(riverType)
                    .setSpec(spec).setSettings(mySettings);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return feeder;
    }

    @Override
    public void start() {
        feeder.setClient(client);
        feeder.setRiverState(new RiverState()
                        .setEnabled(true)
                        .setStarted(new Date())
                        .setName(riverName.getName())
                        .setType(riverName.getType())
                        .setCoordinates("_river", riverName.getName(), "_custom")
        );
        this.riverThread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                "river(" + riverName().getType() + "/" + riverName().getName() + ")")
                .newThread(feeder);
        feeder.schedule(riverThread);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        logger.info("closing river({}/{})", riverName.getType(), riverName.getName());
        try {
            feeder.getRiverState().setEnabled(false).save(client);
            feeder.shutdown();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        if (riverThread != null) {
            riverThread.interrupt();
        }
    }

    @Override
    public RiverState getRiverState() {
        return feeder != null ? feeder.getRiverState() : null;
    }

    @Override
    public void run() {
        feeder.run();
    }
}
