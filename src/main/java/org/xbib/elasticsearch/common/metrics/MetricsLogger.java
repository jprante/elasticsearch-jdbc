/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.common.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.xbib.elasticsearch.common.util.FormatUtil;

import java.text.NumberFormat;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class MetricsLogger {

    private final static Logger plainsourcelogger = LogManager.getLogger("metrics.source.plain");

    private final static Logger plainsinklogger = LogManager.getLogger("metrics.sink.plain");

    private final static Logger jsonsourcelogger = LogManager.getLogger("metrics.source.json");

    private final static Logger jsonsinklogger = LogManager.getLogger("metrics.sink.json");

    private final static NumberFormat formatter = NumberFormat.getNumberInstance();

    public void writeMetrics(Settings settings, SinkMetric metric) throws Exception {
        long submitted = metric.getSubmitted().getCount();
        long succeeded = metric.getSucceeded().getCount();
        long failed = metric.getFailed().getCount();
        long elapsed = metric.elapsed() / 1000000;
        long bytes = metric.getTotalIngestSizeInBytes().getCount();
        double dps = submitted * 1000.0 / elapsed;
        double avg = bytes / (submitted + 1); // avoid div by zero
        double mbps = (bytes * 1024.0 / elapsed) / 1048576.0;
        if (settings.getAsBoolean("metrics.logger.json", false)) {
            XContentBuilder builder = jsonBuilder();
            builder.startObject()
                    .field("elapsed", elapsed)
                    .field("submitted", submitted)
                    .field("succeeded", succeeded)
                    .field("failed", failed)
                    .field("bytes", bytes)
                    .field("avg", avg)
                    .field("dps", dps)
                    .field("mbps", mbps)
                    .endObject();
            jsonsinklogger.info(builder.string());
        }
        if (settings.getAsBoolean("metrics.logger.plain", true)) {
            plainsinklogger.info("{} = {} ms, submitted = {}, succeeded = {}, failed = {}, {} = {} bytes, {} = {} avg size, {} dps, {} MB/s",
                    FormatUtil.formatDurationWords(elapsed, true, true),
                    elapsed,
                    submitted,
                    succeeded,
                    failed,
                    bytes,
                    FormatUtil.convertFileSize(bytes),
                    FormatUtil.convertFileSize(avg),
                    formatter.format(avg),
                    formatter.format(dps),
                    formatter.format(mbps));
        }
    }

    public void writeMetrics(Settings settings, SourceMetric metric) throws Exception {
        long totalrows = metric.getTotalRows().count();
        long elapsed = metric.elapsed() / 1000000;
        long bytes = metric.getTotalSizeInBytes().count();
        double dps = totalrows * 1000.0 / elapsed;
        double avg = bytes / (totalrows + 1); // avoid div by zero
        double mbps = (bytes * 1024.0 / elapsed) / 1048576.0;
        if (settings.getAsBoolean("metrics.logger.json", false)) {
            XContentBuilder builder = jsonBuilder();
            builder.startObject()
                    .field("totalrows", totalrows)
                    .field("elapsed", elapsed)
                    .field("bytes", bytes)
                    .field("avg", avg)
                    .field("dps", dps)
                    .field("mbps", mbps)
                    .endObject();
            jsonsourcelogger.info(builder.string());
        }
        if (settings.getAsBoolean("metrics.logger.plain", true)) {
            plainsourcelogger.info("totalrows = {}, {} = {} ms, {} = {} bytes, {} = {} avg size, {} dps, {} MB/s",
                    totalrows,
                    FormatUtil.formatDurationWords(elapsed, true, true),
                    elapsed,
                    bytes,
                    FormatUtil.convertFileSize(bytes),
                    FormatUtil.convertFileSize(avg),
                    formatter.format(avg),
                    formatter.format(dps),
                    formatter.format(mbps));
        }
    }

}