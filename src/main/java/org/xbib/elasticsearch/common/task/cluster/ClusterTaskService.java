/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.common.task.cluster;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.common.task.Task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 * The TaskService manages reading and writing of custom states in the cluster state
 */
public class ClusterTaskService extends AbstractLifecycleComponent<ClusterTaskService> implements ClusterStateListener {

    private final static ESLogger logger = ESLoggerFactory.getLogger("task");

    private volatile ImmutableMap<String, Task> tasks = ImmutableMap.of();

    private final Injector injector;

    private ClusterService clusterService;

    @Inject
    public ClusterTaskService(Settings settings, Injector injector) {
        super(settings);
        this.injector = injector;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        this.clusterService = injector.getInstance(ClusterService.class);
        clusterService.add(this);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            ClusterTaskMetaData prev = event.previousState().getMetaData().custom(ClusterTaskMetaData.TYPE);
            ClusterTaskMetaData curr = event.state().getMetaData().custom(ClusterTaskMetaData.TYPE);
            if (prev == null) {
                if (curr != null) {
                    processTaskMetadata(curr);
                }
            } else {
                if (!prev.equals(curr)) {
                    processTaskMetadata(curr);
                }
            }
        } catch (Throwable t) {
            logger.warn("failed to update task", t);
        }
    }

    private void processTaskMetadata(ClusterTaskMetaData metaData) {
        Map<String, Task> survivors = newHashMap();
        // first, remove states that are no longer there
        for (Map.Entry<String, Task> entry : tasks.entrySet()) {
            if (metaData != null) {
                if (!metaData.getTask(entry.getKey()).isEmpty()) {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }
        }
        ImmutableMap.Builder<String, Task> builder = ImmutableMap.builder();
        if (metaData != null) {
            for (Task newState : metaData.getTasks()) {
                if (newState.getName() == null) {
                    continue;
                }
                String name = newState.getName();
                Task oldState = survivors.get(name);
                oldState = newState;
                builder.put(name, oldState);
            }

        }
        this.tasks = builder.build();
    }

    /**
     * Put a new task into task management
     *
     * @param request  a task request
     * @param listener listener for cluster state update response
     */
    public void putTask(final TaskRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                // sanity check
                if (request.task.getName() == null) {
                    logger.debug("put: no name given");
                    return currentState;
                }
                String name = request.task.getName();
                Task previous = tasks.get(name);
                if (previous != null) {
                    logger.debug("put: previous state not null");
                    return currentState;
                }
                Map<String, Task> newTasks = newHashMap();
                newTasks.put(name, request.task);
                tasks = ImmutableMap.copyOf(newTasks);
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                ClusterTaskMetaData tasks = currentState.metaData().custom(ClusterTaskMetaData.TYPE);
                if (tasks == null) {
                    logger.debug("put: first task [{}]", request.task.getName());
                    tasks = new ClusterTaskMetaData(request.task);
                } else {
                    boolean found = false;
                    List<Task> taskList = newLinkedList();
                    for (Task state : tasks.getTasks()) {
                        if (state != null
                                && state.getName() != null
                                && state.getName().equals(request.task.getName())) {
                            found = true;
                            taskList.add(request.task);
                        } else {
                            taskList.add(state);
                        }
                    }
                    if (!found && request.task.getName() != null) {
                        logger.debug("put: another task [{}]", request.task.getName());
                        taskList.add(request.task);
                    } else {
                        logger.debug("put: update existing task [{}]", request.task.getName());
                    }
                    tasks = new ClusterTaskMetaData(taskList.toArray(new Task[taskList.size()]));
                }
                metadataBuilder.putCustom(ClusterTaskMetaData.TYPE, tasks);
                return ClusterState.builder(currentState).metaData(metadataBuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failed to create tasks [{}]", t, request.task.getName());
                super.onFailure(source, t);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }

    /**
     * Post a new task for task management
     *
     * @param request  a task register request
     * @param listener listener for cluster task update response
     */
    public void postTask(final TaskRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                // sanity check
                if (request.task.getName() == null) {
                    logger.debug("post: no name given");
                    return currentState;
                }
                String name = request.task.getName();
                Map<String, Task> newTasks = newHashMap();
                newTasks.put(name, request.task);
                tasks = ImmutableMap.copyOf(newTasks);
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                ClusterTaskMetaData metaData = currentState.metaData().custom(ClusterTaskMetaData.TYPE);
                if (metaData == null) {
                    logger.debug("post: first task [{}]", request.task.getName());
                    metaData = new ClusterTaskMetaData(request.task);
                } else {
                    boolean found = false;
                    List<Task> taskList = newLinkedList();
                    for (Task task : metaData.getTasks()) {
                        if (task.getName() != null && task.getName().equals(request.task.getName())) {
                            found = true;
                            if (task.getMap() == null) {
                                task.setMap(new HashMap<String, Object>());
                            }
                            task.setStarted(request.task.getStarted());
                            task.setLastActive(request.task.getLastActiveBegin(), request.task.getLastActiveEnd());
                            if (request.task.getMap() != null) {
                                task.getMap().putAll(request.task.getMap());
                            }
                            logger.debug("post: update task = {}", task);
                            taskList.add(task);
                        } else {
                            taskList.add(task);
                        }
                    }
                    if (!found && request.task.getName() != null) {
                        logger.debug("post: another taks [{}]", request.task.getName());
                        taskList.add(request.task);
                    }
                    metaData = new ClusterTaskMetaData(taskList.toArray(new Task[taskList.size()]));
                }
                metadataBuilder.putCustom(ClusterTaskMetaData.TYPE, metaData);
                return ClusterState.builder(currentState).metaData(metadataBuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failed to post task [{}/{}]", t, request.task.getName());
                super.onFailure(source, t);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }


    /**
     * Delete task from task management
     *
     * @param request  the unregister task request
     * @param listener listener for cluster state updates
     */
    public void deleteTask(final DeleteTaskRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                ClusterTaskMetaData metaData = currentState.metaData().custom(ClusterTaskMetaData.TYPE);
                if (metaData != null && metaData.getTasks().size() > 0) {
                    List<Task> tasks = newLinkedList();
                    boolean changed = false;
                    for (Task task : metaData.getTasks()) {
                        if (Regex.simpleMatch(request.name, task.getName())) {
                            logger.debug("delete: task [{}]", task.getName());
                            changed = true;
                        } else {
                            tasks.add(task);
                        }
                    }
                    if (changed) {
                        metaData = new ClusterTaskMetaData(tasks.toArray(new Task[tasks.size()]));
                        metadataBuilder.putCustom(ClusterTaskMetaData.TYPE, metaData);
                        return ClusterState.builder(currentState).metaData(metadataBuilder).build();
                    }
                }
                throw new ElasticsearchException("unable to delete, state missing: " + request.name);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }

    public static class TaskRequest extends ClusterStateUpdateRequest<TaskRequest> {

        final String cause;

        final Task task;

        /**
         * Constructs new register request
         *
         * @param cause registration cause
         * @param task state
         */
        public TaskRequest(String cause, Task task) {
            this.cause = cause;
            this.task = task;
        }

    }

    public static class DeleteTaskRequest extends ClusterStateUpdateRequest<DeleteTaskRequest> {

        final String cause;

        final String name;

        /**
         * Creates a new delete task request
         *
         * @param cause delete cause
         * @param name  name
         */
        public DeleteTaskRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }
    }

}
