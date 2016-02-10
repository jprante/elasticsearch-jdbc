package org.xbib.metrics;

public interface MeanMetric extends Metric {

    void inc(long n);

    void dec(long n);

    long count();

    long sum();

    double mean() ;

    void clear();
}
