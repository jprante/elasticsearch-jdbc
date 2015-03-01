package org.xbib.elasticsearch.common.task.worker;

import org.elasticsearch.common.inject.Module;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 * Registry for keeping all Worker modules and Worker instances
 */
public class WorkerRegistry {

    private final Map<String, Collection<Class<? extends Module>>> workerModules;

    private final Map<String, Worker> workerMap;

    public WorkerRegistry(Map<String, Collection<Class<? extends Module>>> workerModules) {
        this.workerModules = workerModules;
        this.workerMap = newHashMap();
    }

    public Collection<Class<? extends Module>> getWorkerModules(String name) {
        return workerModules.get(name);
    }

    public Map<String, Collection<Class<? extends Module>>> getWorkerModules() {
        return workerModules;
    }

    public void addWorker(String name, Worker worker) {
        workerMap.put(name, worker);
        workerModules.put(name, worker.modules());
    }

    public Map<String, Worker> getWorkerMap() {
        return workerMap;
    }

    public String toString() {
        return workerMap.toString();
    }

}
