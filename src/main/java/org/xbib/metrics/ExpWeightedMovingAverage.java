package org.xbib.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * An exponentially-weighted moving average.
 *
 * @see <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf">UNIX Load Average Part 1: How It Works</a>
 * @see <a href="http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf">UNIX Load Average Part 2: Not Your Average Average</a>
 */
public class ExpWeightedMovingAverage {
    private static final double M1_ALPHA = 1 - Math.exp(-5 / 60.0);
    private static final double M5_ALPHA = 1 - Math.exp(-5 / 60.0 / 5);
    private static final double M15_ALPHA = 1 - Math.exp(-5 / 60.0 / 15);
    private final LongAdder uncounted = new LongAdder();
    private final double alpha, interval;
    private volatile boolean initialized = false;
    private volatile double rate = 0.0;

    /**
     * Create a new EWMA with a specific smoothing constant.
     *
     * @param alpha        the smoothing constant
     * @param interval     the expected tick interval
     * @param intervalUnit the time unit of the tick interval
     */
    public ExpWeightedMovingAverage(double alpha, long interval, TimeUnit intervalUnit) {
        this.interval = intervalUnit.toNanos(interval);
        this.alpha = alpha;
    }

    /**
     * Creates a new EWMA which is equivalent to the UNIX one minute load average and which expects to be ticked every
     * 5 seconds.
     *
     * @return a one-minute EWMA
     */
    public static ExpWeightedMovingAverage oneMinuteEWMA() {
        return new ExpWeightedMovingAverage(M1_ALPHA, 5, TimeUnit.SECONDS);
    }

    /**
     * Creates a new EWMA which is equivalent to the UNIX five minute load average and which expects to be ticked every
     * 5 seconds.
     *
     * @return a five-minute EWMA
     */
    public static ExpWeightedMovingAverage fiveMinuteEWMA() {
        return new ExpWeightedMovingAverage(M5_ALPHA, 5, TimeUnit.SECONDS);
    }

    /**
     * Creates a new EWMA which is equivalent to the UNIX fifteen minute load average and which expects to be ticked
     * every 5 seconds.
     *
     * @return a fifteen-minute EWMA
     */
    public static ExpWeightedMovingAverage fifteenMinuteEWMA() {
        return new ExpWeightedMovingAverage(M15_ALPHA, 5, TimeUnit.SECONDS);
    }

    /**
     * Update the moving average with a new value.
     *
     * @param n the new value
     */
    public void update(long n) {
        uncounted.add(n);
    }

    /**
     * Mark the passage of time and decay the current rate accordingly.
     */
    public void tick() {
        final long count = uncounted.sumThenReset();
        double instantRate = count / interval;
        if (initialized) {
            rate += (alpha * (instantRate - rate));
        } else {
            rate = instantRate;
            initialized = true;
        }
    }

    /**
     * Returns the rate in the given units of time.
     *
     * @param rateUnit the unit of time
     * @return the rate
     */
    public double rate(TimeUnit rateUnit) {
        return rate * (double) rateUnit.toNanos(1);
    }
}
