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
package org.xbib.elasticsearch.common.state.cluster;

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
import org.xbib.elasticsearch.common.state.State;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 * The StateService manages reading and writing of custom states in the cluster state
 */
public class StateService extends AbstractLifecycleComponent<StateService> implements ClusterStateListener {

    private final static ESLogger logger = ESLoggerFactory.getLogger("jdbc");

    private volatile ImmutableMap<String, State> states = ImmutableMap.of();

    private final Injector injector;

    private ClusterService clusterService;

    @Inject
    public StateService(Settings settings, Injector injector) {
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
            StateMetaData prev = event.previousState().getMetaData().custom(StateMetaData.TYPE);
            StateMetaData curr = event.state().getMetaData().custom(StateMetaData.TYPE);
            if (prev == null) {
                if (curr != null) {
                    processStatesMetadata(curr);
                }
            } else {
                if (!prev.equals(curr)) {
                    processStatesMetadata(curr);
                }
            }
        } catch (Throwable t) {
            logger.warn("failed to update state", t);
        }
    }

    private void processStatesMetadata(StateMetaData stateMetaData) {
        Map<String, State> survivors = newHashMap();
        // first, remove states that are no longer there
        for (Map.Entry<String, State> entry : states.entrySet()) {
            if (stateMetaData != null) {
                if (!stateMetaData.getStates(entry.getKey()).isEmpty()) {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }
        }
        ImmutableMap.Builder<String, State> builder = ImmutableMap.builder();
        if (stateMetaData != null) {
            for (State newState : stateMetaData.getStates()) {
                if (newState.getName() == null) {
                    continue;
                }
                String name = newState.getName();
                State oldState = survivors.get(name);
                oldState = newState;
                builder.put(name, oldState);
            }

        }
        this.states = builder.build();
    }

    /**
     * Put a new state into state management
     *
     * @param request  a state register request
     * @param listener listener for cluster state update response
     */
    public void putState(final StateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                // sanity check
                if (request.state.getName() == null) {
                    logger.debug("put: no name given");
                    return currentState;
                }
                String name = request.state.getName();
                State previous = states.get(name);
                if (previous != null) {
                    logger.debug("put: previous state not null");
                    return currentState;
                }
                Map<String, State> newStates = newHashMap();
                newStates.put(name, request.state);
                states = ImmutableMap.copyOf(newStates);
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                StateMetaData states = currentState.metaData().custom(StateMetaData.TYPE);
                if (states == null) {
                    logger.debug("put: first state [{}]", request.state.getName());
                    states = new StateMetaData(request.state);
                } else {
                    boolean found = false;
                    List<State> stateList = newLinkedList();
                    for (State state : states.getStates()) {
                        if (state != null
                                && request.state != null && state.getName() != null
                                && state.getName().equals(request.state.getName())) {
                            found = true;
                            stateList.add(request.state);
                        } else {
                            stateList.add(state);
                        }
                    }
                    if (!found && request.state != null && request.state.getName() != null) {
                        logger.debug("put: another state [{}]", request.state.getName());
                        stateList.add(request.state);
                    } else {
                        logger.debug("put: update existing state [{}]", request.state.getName());
                    }
                    states = new StateMetaData(stateList.toArray(new State[stateList.size()]));
                }
                metadataBuilder.putCustom(StateMetaData.TYPE, states);
                return ClusterState.builder(currentState).metaData(metadataBuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failed to create state [{}]", t, request.state.getName());
                super.onFailure(source, t);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }

    /**
     * Post a new state for state management
     *
     * @param request  a state register request
     * @param listener listener for cluster state update response
     */
    public void postState(final StateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                // sanity check
                if (request.state.getName() == null) {
                    logger.debug("post: no name given");
                    return currentState;
                }
                String name = request.state.getName();
                Map<String, State> newStates = newHashMap();
                newStates.put(name, request.state);
                states = ImmutableMap.copyOf(newStates);
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                StateMetaData stateMetaData = currentState.metaData().custom(StateMetaData.TYPE);
                if (stateMetaData == null) {
                    logger.debug("post: first state [{}]", request.state.getName());
                    stateMetaData = new StateMetaData(request.state);
                } else {
                    boolean found = false;
                    List<State> stateList = newLinkedList();
                    for (State state : stateMetaData.getStates()) {
                        if (state.getName() != null && state.getName().equals(request.state.getName())) {
                            found = true;
                            if (state.getMap() == null) {
                                state.setMap(new HashMap<String, Object>());
                            }
                            if (request.state != null) {
                                state.setStarted(request.state.getStarted());
                                state.setLastActive(request.state.getLastActiveBegin(), request.state.getLastActiveEnd());
                                if (request.state.getMap() != null) {
                                    state.getMap().putAll(request.state.getMap());
                                }
                            }
                            logger.debug("post: update state = {}", state);
                            stateList.add(state);
                        } else {
                            stateList.add(state);
                        }
                    }
                    if (!found && request.state != null && request.state.getName() != null) {
                        logger.debug("post: another state [{}]", request.state.getName());
                        stateList.add(request.state);
                    }
                    stateMetaData = new StateMetaData(stateList.toArray(new State[stateList.size()]));
                }
                metadataBuilder.putCustom(StateMetaData.TYPE, stateMetaData);
                return ClusterState.builder(currentState).metaData(metadataBuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failed to post state [{}/{}]", t, request.state.getName());
                super.onFailure(source, t);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }


    /**
     * Delete state from state management
     *
     * @param request  the unregister state request
     * @param listener listener for cluster state updates
     */
    public void deleteState(final DeleteStateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                StateMetaData stateMetaData = currentState.metaData().custom(StateMetaData.TYPE);
                if (stateMetaData != null && stateMetaData.getStates().size() > 0) {
                    List<State> states = newLinkedList();
                    boolean changed = false;
                    for (State state : stateMetaData.getStates()) {
                        if (Regex.simpleMatch(request.name, state.getName())) {
                            logger.debug("delete: state [{}]", state.getName());
                            changed = true;
                        } else {
                            states.add(state);
                        }
                    }
                    if (changed) {
                        stateMetaData = new StateMetaData(states.toArray(new State[states.size()]));
                        metadataBuilder.putCustom(StateMetaData.TYPE, stateMetaData);
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

    public static class StateRequest extends ClusterStateUpdateRequest<StateRequest> {

        final String cause;

        State state;

        /**
         * Constructs new register request
         *
         * @param cause registration cause
         * @param state state
         */
        public StateRequest(String cause, State state) {
            this.cause = cause;
            this.state = state;
        }

    }

    public static class DeleteStateRequest extends ClusterStateUpdateRequest<DeleteStateRequest> {

        final String cause;

        final String name;

        /**
         * Creates a new delete state request
         *
         * @param cause delete cause
         * @param name  name
         */
        public DeleteStateRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }
    }

}
