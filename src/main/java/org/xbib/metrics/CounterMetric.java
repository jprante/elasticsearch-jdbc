package org.xbib.metrics;

public interface CounterMetric {

    void inc();

    void inc(long n);

    void dec();

    void dec(long n);

    long count();
}
