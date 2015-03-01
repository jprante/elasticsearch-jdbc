package org.xbib.elasticsearch.common.task.node;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * The task service manages the task registry.
 *
 * Tasks are announced to the node attributes so they can be seen at node discovery time.
 */
public class TaskService extends AbstractLifecycleComponent<TaskService> {

    private final TaskRegistry registry;

    private final NodeService nodeService;

    @Inject
    public TaskService(Settings settings,
                       TaskRegistry registry,
                       NodeService nodeService) {
        super(settings);
        this.registry = registry;
        this.nodeService = nodeService;
    }

    public TaskRegistry getTaskRegistry() {
        return registry;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        nodeService.putAttribute(TaskConstants.TASK_COUNT, Integer.toString(registry.getTasks().size()));
        ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor(daemonThreadFactory(settings, "task_count_watcher"));
        executorService.scheduleAtFixedRate(new Thread() {
            public void run() {
                nodeService.putAttribute(TaskConstants.TASK_COUNT, Integer.toString(registry.getTasks().size()));
            }
        }, 1L, 5L, TimeUnit.SECONDS);
        logger.info("started");
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        nodeService.removeAttribute(TaskConstants.TASK_COUNT);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

}
