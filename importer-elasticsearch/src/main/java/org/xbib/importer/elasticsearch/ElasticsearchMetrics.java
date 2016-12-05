package org.xbib.importer.elasticsearch;

import org.xbib.content.settings.Settings;
import org.xbib.elasticsearch.extras.client.BulkMetric;
import org.xbib.importer.util.FormatUtil;
import org.xbib.metrics.Meter;

import java.io.Writer;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class ElasticsearchMetrics {

    private static final Logger logger = Logger.getLogger(ElasticsearchMetrics.class.getName());

    protected final Map<String, MetricWriter> writers;

    protected final ScheduledExecutorService service;

    public ElasticsearchMetrics() {
        this.writers = new HashMap<>();
        this.service = Executors.newScheduledThreadPool(2);
    }

    public ScheduledExecutorService getService() {
        return service;
    }


    public void schedule(Settings settings, String type, Meter metric) {
        if (settings == null) {
            logger.log(Level.WARNING, "can not schedule, no settings");
            return;
        }
        if (type == null || !type.startsWith("meter")) {
            logger.log(Level.WARNING, "can not schedule, not a metric type that starts with meter*");
            return;
        }
        // run every 10 seconds by default
        long value = settings.getAsLong("schedule.metrics.seconds", 10L);
        service.scheduleAtFixedRate(new MeterRunnable(type, metric), 0L, value, TimeUnit.SECONDS);
        logger.log(Level.INFO, "scheduled metrics at interval of {} seconds", value);
    }

    public synchronized void append(String type, Meter metric) {
        if (metric == null) {
            return;
        }
        long docs = metric.getCount();
        long elapsed = metric.elapsed() / 1000000; // nanos to millis
        double dps = docs * 1000.0 / elapsed;
        long mean = Math.round(metric.getMeanRate());
        long oneminute = Math.round(metric.getOneMinuteRate());
        long fiveminute = Math.round(metric.getFiveMinuteRate());
        long fifteenminute = Math.round(metric.getFifteenMinuteRate());

        logger.log(Level.INFO, MessageFormat.format("{0}: {1} docs, {2} ms = {3}, {4} = {5}, {6} ({7} {8} {9})",
                type,
                docs,
                elapsed,
                FormatUtil.formatDurationWords(elapsed, true, true),
                dps,
                FormatUtil.formatDocumentSpeed(dps),
                mean,
                oneminute,
                fiveminute,
                fifteenminute
        ));

        for (Map.Entry<String, MetricWriter> entry : writers.entrySet()) {
            try {
                MetricWriter metricWriter = entry.getValue();
                if (type.equals(metricWriter.getType()) && metricWriter.getWriter() != null) {
                    Settings settings = metricWriter.getSettings();
                    Locale locale = metricWriter.getLocale();
                    String format = settings.get("format", "%s\t%d\t%d\n");
                    String message = String.format(locale, format, metricWriter.getTitle(), elapsed, docs);
                    metricWriter.getWriter().write(message);
                    metricWriter.getWriter().flush();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    /**
     *
     */
    protected static class MetricWriter {
        private String type;
        private String title;
        private Path path;
        private Path chart;
        private Writer writer;
        private Settings settings;
        private Locale locale;

        public void setType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getTitle() {
            return title;
        }

        public void setPath(Path path) {
            this.path = path;
        }

        public Path getPath() {
            return path;
        }

        public void setChart(Path chart) {
            this.chart = chart;
        }

        public Path getChart() {
            return chart;
        }

        public void setWriter(Writer writer) {
            this.writer = writer;
        }

        public Writer getWriter() {
            return writer;
        }

        public void setSettings(Settings settings) {
            this.settings = settings;
        }

        public Settings getSettings() {
            return settings;
        }

        public void setLocale(Locale locale) {
            this.locale = locale;
        }

        public Locale getLocale() {
            return locale;
        }
    }

    private class MeterRunnable implements Runnable {

        private final String type;

        private final Meter metric;

        MeterRunnable(String type, Meter meterMetric) {
            this.type = type;
            this.metric = meterMetric;
        }

        @Override
        public void run() {
            append(type, metric);
        }
    }

    public void scheduleBulkMetric(Settings settings, BulkMetric bulkMetric) {
        if (settings == null) {
            logger.log(Level.WARNING, "no settings");
            return;
        }
        if (bulkMetric == null) {
            logger.log(Level.WARNING, "no bulk metric");
            return;
        }
        // run every 10 seconds by default
        long value = settings.getAsLong("schedule.metrics.seconds", 10L);
        service.scheduleAtFixedRate(new BulkMetricRunnable("bulk", bulkMetric), 0L, value, TimeUnit.SECONDS);
        logger.log(Level.INFO, "scheduled ingest metrics at " + value + " seconds");
    }

    public synchronized void append(String type, BulkMetric metric) {
        if (metric == null) {
            return;
        }
        long docs = metric.getSucceeded().getCount();
        long elapsed = metric.elapsed() / 1000000; // nano to millis
        double dps = docs * 1000.0 / elapsed;
        long bytes = metric.getTotalIngestSizeInBytes().getCount();
        double avg = bytes / (docs + 1.0); // avoid div by zero
        double bps = bytes * 1000.0 / elapsed;

        logger.log(Level.INFO, MessageFormat.format("{0}: {1} docs, {2} ms = {3}, {4} = {5}, {6} = {7} avg, {8} = {9}, {10} = {11}",
                type,
                docs,
                elapsed,
                FormatUtil.formatDurationWords(elapsed, true, true),
                bytes,
                FormatUtil.formatSize(bytes),
                avg,
                FormatUtil.formatSize(avg),
                dps,
                FormatUtil.formatDocumentSpeed(dps),
                bps,
                FormatUtil.formatSpeed(bps)
        ));

        for (Map.Entry<String, MetricWriter> entry : writers.entrySet()) {
            try {
                MetricWriter writer = entry.getValue();
                if (type.equals(writer.getType()) && writer.getWriter() != null) {
                    Settings settings = writer.getSettings();
                    Locale locale = writer.getLocale();
                    String format = settings.get("format", "%s\t%d\t%d\t%d\n");
                    String message = String.format(locale, format, writer.getTitle(), elapsed, bytes, docs);
                    writer.getWriter().write(message);
                    writer.getWriter().flush();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    private class BulkMetricRunnable implements Runnable {

        private final String type;

        private final BulkMetric metric;

        BulkMetricRunnable(String type, BulkMetric metric) {
            this.type = type;
            this.metric = metric;
        }

        @Override
        public void run() {
            append(type, metric);
        }
    }

}
