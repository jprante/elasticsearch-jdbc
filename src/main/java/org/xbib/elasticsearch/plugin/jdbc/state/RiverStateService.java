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
package org.xbib.elasticsearch.plugin.jdbc.state;

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
import org.elasticsearch.river.RiverName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 * The RiverStateService manages reading and writing of river states in the cluster state
 */
public class RiverStateService extends AbstractLifecycleComponent<RiverStateService> implements ClusterStateListener {

    private final static ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.RiverStateService");

    private volatile ImmutableMap<RiverName, RiverState> riverStates = ImmutableMap.of();

    private final Injector injector;

    private ClusterService clusterService;

    @Inject
    public RiverStateService(Settings settings, Injector injector) {
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
            RiverStatesMetaData prev = event.previousState().getMetaData().custom(RiverStatesMetaData.TYPE);
            RiverStatesMetaData curr = event.state().getMetaData().custom(RiverStatesMetaData.TYPE);
            if (prev == null) {
                if (curr != null) {
                    processRiverStatesMetadata(curr);
                }
            } else {
                if (!prev.equals(curr)) {
                    processRiverStatesMetadata(curr);
                }
            }
        } catch (Throwable t) {
            logger.warn("failed to update river state", t);
        }
    }

    private void processRiverStatesMetadata(RiverStatesMetaData riverStatesMetaData) {
        Map<RiverName, RiverState> survivors = newHashMap();
        // first, remove river states that are no longer there
        for (Map.Entry<RiverName, RiverState> entry : riverStates.entrySet()) {
            if (riverStatesMetaData != null) {
                if (!riverStatesMetaData.getRiverStates(entry.getKey().getName(), entry.getKey().getType()).isEmpty()) {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }
        }
        ImmutableMap.Builder<RiverName, RiverState> builder = ImmutableMap.builder();
        if (riverStatesMetaData != null) {
            for (RiverState newRiverState : riverStatesMetaData.getRiverStates()) {
                if (newRiverState.getName() == null || newRiverState.getType() == null) {
                    continue;
                }
                RiverName riverName = new RiverName(newRiverState.getType(), newRiverState.getName());
                RiverState oldRiverState = survivors.get(riverName);
                if (oldRiverState != null && oldRiverState.getType() != null) {
                    if (!oldRiverState.getType().equals(newRiverState.getType())) {
                        oldRiverState = newRiverState;
                    }
                } else {
                    oldRiverState = newRiverState;
                }
                builder.put(riverName, oldRiverState);
            }

        }
        this.riverStates = builder.build();
    }

    /**
     * Put a new river into river state management
     *
     * @param request  a river state register request
     * @param listener listener for cluster state update response
     */
    public void putRiverState(final RiverStateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                // sanity check
                if (request.riverState.getName() == null || request.riverState.getType() == null) {
                    logger.debug("put: no river name / type given");
                    return currentState;
                }
                RiverName riverName = new RiverName(request.riverState.getType(), request.riverState.getName());
                RiverState previous = riverStates.get(riverName);
                if (previous != null) {
                    logger.debug("put: previous state not null");
                    return currentState;
                }
                Map<RiverName, RiverState> newRiverStates = newHashMap();
                newRiverStates.put(riverName, request.riverState);
                riverStates = ImmutableMap.copyOf(newRiverStates);
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                RiverStatesMetaData riverStates = currentState.metaData().custom(RiverStatesMetaData.TYPE);
                if (riverStates == null) {
                    logger.debug("put: first river state [{}/{}]", request.riverState.getType(), request.riverState.getName());
                    riverStates = new RiverStatesMetaData(request.riverState);
                } else {
                    boolean found = false;
                    List<RiverState> riverStateList = newLinkedList();
                    for (RiverState riverState : riverStates.getRiverStates()) {
                        if (riverState != null
                                && request.riverState != null && riverState.getName() != null
                                && riverState.getName().equals(request.riverState.getName())
                                && riverState.getType() != null
                                && riverState.getType().equals(request.riverState.getType())) {
                            found = true;
                            riverStateList.add(request.riverState);
                        } else {
                            riverStateList.add(riverState);
                        }
                    }
                    if (!found && request.riverState != null && request.riverState.getName() != null) {
                        logger.debug("put: another river state [{}/{}]", request.riverState.getType(), request.riverState.getName());
                        riverStateList.add(request.riverState);
                    } else {
                        logger.debug("put: update existing river state [{}/{}]", request.riverState.getType(), request.riverState.getName());
                    }
                    riverStates = new RiverStatesMetaData(riverStateList.toArray(new RiverState[riverStateList.size()]));
                }
                metadataBuilder.putCustom(RiverStatesMetaData.TYPE, riverStates);
                return ClusterState.builder(currentState).metaData(metadataBuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failed to create river state [{}/{}]", t, request.riverState.getType(), request.riverState.getName());
                super.onFailure(source, t);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }

    /**
     * Post a new river for river state management
     *
     * @param request  a river state register request
     * @param listener listener for cluster state update response
     */
    public void postRiverState(final RiverStateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                // sanity check
                if (request.riverState.getName() == null || request.riverState.getType() == null) {
                    logger.debug("post: no river name / type given");
                    return currentState;
                }
                RiverName riverName = new RiverName(request.riverState.getType(), request.riverState.getName());
                Map<RiverName, RiverState> newRiverStates = newHashMap();
                newRiverStates.put(riverName, request.riverState);
                riverStates = ImmutableMap.copyOf(newRiverStates);
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                RiverStatesMetaData riverStatesMetaData = currentState.metaData().custom(RiverStatesMetaData.TYPE);
                if (riverStatesMetaData == null) {
                    logger.debug("post: first river state [{}/{}]", request.riverState.getType(), request.riverState.getName());
                    riverStatesMetaData = new RiverStatesMetaData(request.riverState);
                } else {
                    boolean found = false;
                    List<RiverState> riverStateList = newLinkedList();
                    for (RiverState riverState : riverStatesMetaData.getRiverStates()) {
                        if (riverState.getName() != null && riverState.getName().equals(request.riverState.getName())
                                && riverState.getType() != null && riverState.getType().equals(request.riverState.getType())) {
                            found = true;
                            if (riverState.getMap() == null) {
                                riverState.setMap(new HashMap<String, Object>());
                            }
                            if (request.riverState != null) {
                                riverState.setStarted(request.riverState.getStarted());
                                riverState.setLastActive(request.riverState.getLastActiveBegin(), request.riverState.getLastActiveEnd());
                                if (request.riverState.getMap() != null) {
                                    riverState.getMap().putAll(request.riverState.getMap());
                                }
                            }
                            logger.debug("post: update river state = {}", riverState);
                            riverStateList.add(riverState);
                        } else {
                            riverStateList.add(riverState);
                        }
                    }
                    if (!found && request.riverState != null && request.riverState.getName() != null) {
                        logger.debug("post: another river state [{}/{}]", request.riverState.getType(), request.riverState.getName());
                        riverStateList.add(request.riverState);
                    }
                    riverStatesMetaData = new RiverStatesMetaData(riverStateList.toArray(new RiverState[riverStateList.size()]));
                }
                metadataBuilder.putCustom(RiverStatesMetaData.TYPE, riverStatesMetaData);
                return ClusterState.builder(currentState).metaData(metadataBuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failed to post river state [{}/{}]", t, request.riverState.getType(), request.riverState.getName());
                super.onFailure(source, t);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }


    /**
     * Delete river from river state management
     *
     * @param request  the unregister river state request
     * @param listener listener for cluster state updates
     */
    public void deleteRiverState(final DeleteRiverStateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                RiverStatesMetaData riverStatesMetaData = currentState.metaData().custom(RiverStatesMetaData.TYPE);
                if (riverStatesMetaData != null && riverStatesMetaData.getRiverStates().size() > 0) {
                    List<RiverState> riverStates = newLinkedList();
                    boolean changed = false;
                    for (RiverState riverState : riverStatesMetaData.getRiverStates()) {
                        if (riverState.getType().equals(request.type) && Regex.simpleMatch(request.name, riverState.getName())) {
                            logger.debug("delete: river state [{}]", riverState.getName());
                            changed = true;
                        } else {
                            riverStates.add(riverState);
                        }
                    }
                    if (changed) {
                        riverStatesMetaData = new RiverStatesMetaData(riverStates.toArray(new RiverState[riverStates.size()]));
                        metadataBuilder.putCustom(RiverStatesMetaData.TYPE, riverStatesMetaData);
                        return ClusterState.builder(currentState).metaData(metadataBuilder).build();
                    }
                }
                throw new ElasticsearchException("unable to delete, river state missing: " + request.name);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }

    public static class RiverStateRequest extends ClusterStateUpdateRequest<RiverStateRequest> {

        final String cause;

        RiverState riverState;

        /**
         * Constructs new register river request
         *
         * @param cause      river registration cause
         * @param riverState river state
         */
        public RiverStateRequest(String cause, RiverState riverState) {
            this.cause = cause;
            this.riverState = riverState;
        }

    }

    public static class DeleteRiverStateRequest extends ClusterStateUpdateRequest<DeleteRiverStateRequest> {

        final String cause;

        final String name;

        final String type;

        /**
         * Creates a new delete river state request
         *
         * @param cause river delete cause
         * @param name  river name
         */
        public DeleteRiverStateRequest(String cause, String name, String type) {
            this.cause = cause;
            this.name = name;
            this.type = type;
        }
    }

}
