package org.xbib.metrics;

import com.twitter.jsr166e.LongAdder;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A meter metric which measures mean throughput and one-, five-, and
 * fifteen-minute exponentially-weighted moving average throughputs.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
 */
public class ElasticsearchMeterMetric implements MeterMetric {

    private final static ScheduledExecutorService service = Executors.newScheduledThreadPool(3);
    private final ExpWeightedMovingAverage m1Rate = ExpWeightedMovingAverage.oneMinuteEWMA();
    private final ExpWeightedMovingAverage m5Rate = ExpWeightedMovingAverage.fiveMinuteEWMA();
    private final ExpWeightedMovingAverage m15Rate = ExpWeightedMovingAverage.fifteenMinuteEWMA();
    private final LongAdder count;
    private final long startDate;
    private final long startTime;
    private final TimeUnit rateUnit;
    private final ScheduledFuture<?> future;

    private long stopDate;

    private long stopTime;

    public ElasticsearchMeterMetric(long intervalSeconds, TimeUnit rateUnit) {
        this.rateUnit = rateUnit;
        this.count = new LongAdder();
        this.startDate = System.currentTimeMillis();
        this.startTime = System.nanoTime();
        this.future = service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                tick();
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    public TimeUnit rateUnit() {
        return rateUnit;
    }

    /**
     * Updates the moving averages.
     */
    public void tick() {
        m1Rate.tick();
        m5Rate.tick();
        m15Rate.tick();
    }

    /**
     * Mark the occurrence of an event.
     */
    public void mark() {
        mark(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n) {
        count.add(n);
        m1Rate.update(n);
        m5Rate.update(n);
        m15Rate.update(n);
    }

    public long count() {
        return count.sum();
    }

    public long started() {
        return startTime;
    }

    public Date startedAt() {
        return new Date(startDate);
    }

    public long stopped() {
        return stopTime;
    }

    public Date stoppedAt() {
        return new Date(stopDate);
    }

    public long elapsed() {
        return System.nanoTime() - startTime;
    }

    public double fifteenMinuteRate() {
        return m15Rate.rate(rateUnit);
    }

    public double fiveMinuteRate() {
        return m5Rate.rate(rateUnit);
    }

    public double meanRate() {
        long count = count();
        if (count == 0) {
            return 0.0;
        } else {
            final long elapsed = System.nanoTime() - startTime;
            return convertNsRate(count / (double) elapsed);
        }
    }

    public double oneMinuteRate() {
        return m1Rate.rate(rateUnit);
    }

    public void stop() {
        this.stopTime = System.nanoTime();
        this.stopDate = System.currentTimeMillis();
        future.cancel(false);
    }

    private double convertNsRate(double ratePerNs) {
        return ratePerNs * (double) rateUnit.toNanos(1);
    }

}
