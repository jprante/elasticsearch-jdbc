package org.xbib.tools.jmx;

public interface IndexCounterMBean {

    int getCount();

    void increment();

    void reset();

}
