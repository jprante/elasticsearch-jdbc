package org.xbib.tools.jmx;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexCounter implements IndexCounterMBean {

    private final static Logger log = LogManager.getLogger(IndexCounter.class);

    private AtomicInteger count = new AtomicInteger();

    private static IndexCounter instance;

    static {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("org.xbib.tools.jmx:type=IndexCounter");
            instance = new IndexCounter();
            mbs.registerMBean(instance, name);
        } catch(Exception ex) {
            log.error("Unable to register IndexCounter MBean", ex);
        }
    }

    private IndexCounter() {}

    public int getCount() { return count.get(); }

    public void increment() { count.incrementAndGet(); }

    public void reset() { count.set(0); }

    public static IndexCounter getInstance() {
        return instance;
    }
}