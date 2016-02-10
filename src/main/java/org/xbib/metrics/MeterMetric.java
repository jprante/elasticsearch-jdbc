package org.xbib.metrics;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public interface MeterMetric extends Metric {

    TimeUnit rateUnit();

    /**
     * Updates the moving averages.
     */
    void tick() ;

    /**
     * Mark the occurrence of an event.
     */
    void mark();

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    void mark(long n) ;

    long count();

    long started();

    Date startedAt();

    long stopped();

    Date stoppedAt();

    long elapsed();

    double fifteenMinuteRate();

    double fiveMinuteRate();

    double meanRate() ;

    double oneMinuteRate();

    void stop() ;

}
