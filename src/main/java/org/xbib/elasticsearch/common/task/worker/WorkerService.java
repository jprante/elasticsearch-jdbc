package org.xbib.elasticsearch.common.task.worker;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * The worker service manages the worker workerRegistry.
 *
 * Workers are announced to the node attributes so they can be seen by node discovery.
 */
public class WorkerService extends AbstractLifecycleComponent<WorkerService> {

    private final WorkerRegistry workerRegistry;

    private final NodeService nodeService;

    private final OperatingSystemMXBean operatingSystemMXBean;

    @Inject
    public WorkerService(Settings settings,
                         WorkerRegistry workerRegistry,
                         NodeService nodeService) {
        super(settings);
        this.workerRegistry = workerRegistry;
        this.nodeService = nodeService;
        this.operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
    }

    public WorkerRegistry getWorkerRegistry() {
        return workerRegistry;
    }

    @Override
    protected void doStart() throws ElasticsearchException {

        nodeService.putAttribute(WorkerConstants.WORKER_MODULES,
                Strings.collectionToCommaDelimitedString(workerRegistry.getWorkerMap().keySet()));

        // the load updater task
        ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor(daemonThreadFactory(settings, "worker_load_watcher"));
        executorService.scheduleAtFixedRate(new Thread() {
            public void run() {
                double load = operatingSystemMXBean.getSystemLoadAverage();
                nodeService.putAttribute(WorkerConstants.WORKER_LOAD, Double.toString(load));
            }
        }, 1L, 1L, TimeUnit.MINUTES);

        // the queue length updater task
        executorService.scheduleAtFixedRate(new Thread() {
            public void run() {
                int length = 0; // TODO getPendingJobs();
                nodeService.putAttribute(WorkerConstants.WORKER_COUNT, Integer.toString(length));
            }
        }, 1L, 5L, TimeUnit.SECONDS);

        logger.info("started");
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        nodeService.removeAttribute(WorkerConstants.WORKER_MODULES);
        nodeService.removeAttribute(WorkerConstants.WORKER_LOAD);
        nodeService.removeAttribute(WorkerConstants.WORKER_COUNT);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

}
