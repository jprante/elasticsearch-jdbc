package org.xbib.elasticsearch.common.task.worker;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Modules;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class WorkerModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    private Map<String, Collection<Class<? extends Module>>> workerModules = Maps.newHashMap();

    public WorkerModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(WorkerService.class).asEagerSingleton();
        bind(WorkerRegistry.class).toInstance(new WorkerRegistry(workerModules));
    }

    /**
     * Registers a custom worker module.
     *
     * @param name the name
     * @param module he module
     */
    public void registerWorkerModule(String name, Collection<Class<? extends Module>> module) {
        workerModules.put(name, module);
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        Collection<Module> modules = newLinkedList();
        for (Map.Entry<String, Collection<Class<? extends Module>>> me : workerModules.entrySet()) {
            for (Class<? extends Module> cl : me.getValue()){
                modules.add(Modules.createModule(cl, settings));
            }
        }
        return modules;
    }
}

