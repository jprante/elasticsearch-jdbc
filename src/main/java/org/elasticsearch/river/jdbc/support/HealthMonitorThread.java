package org.elasticsearch.river.jdbc.support;

import java.lang.Runnable;
import java.lang.Thread;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Cluster health monitor thread. Checks the cluster status every 5 minutes and is used by
 * all JDBC river mouths to check if actions should be performed.
 * @author pdegeus
 */
public class HealthMonitorThread implements Runnable {

    private static final ESLogger LOG = ESLoggerFactory.getLogger(HealthMonitorThread.class.getName());

    private boolean stop = false;
    private boolean healthy = false;
    private final Client client;

    /**
     * Constructor.
     * @param client Client instance to check health with.
     */
    public HealthMonitorThread(Client client) {
        this.client = client;
    }

    @Override
    public void run() {
        while (!stop) {
            healthy = false;
            int curTime = 0;

            while (!healthy) {
                ClusterHealthResponse health = client.admin().cluster().prepareHealth()
                    .setWaitForYellowStatus()
                    .setTimeout(TimeValue.timeValueMinutes(1))
                    .execute().actionGet();

                if (health.isTimedOut()) {
                    LOG.info("Waiting for cluster ({} minutes elapsed)...", curTime);
                    curTime++;
                } else {
                    healthy = true;
                }
            }

            try {
                Thread.sleep(TimeValue.timeValueMinutes(5).getMillis());
            } catch (InterruptedException e) {
                Thread.interrupted();
                if (!stop) {
                    LOG.warn("Thread interrupted unexpectedly, stopping", e);
                    stop = true;
                }
            }
        }
    }

    /**
     * Stops this thread.
     */
    public void stop() {
        stop = true;
    }

    /**
     * @return True if the cluster is healthy.
     */
    public boolean isHealthy() {
        return healthy;
    }

}