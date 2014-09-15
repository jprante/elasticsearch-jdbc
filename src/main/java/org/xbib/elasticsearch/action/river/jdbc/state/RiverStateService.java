package org.xbib.elasticsearch.action.river.jdbc.state;

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
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.newLinkedList;
import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 * The RiverStateService manages reading and writing of river states in the cluster state
 */
public class RiverStateService extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;

    private volatile ImmutableMap<String, RiverState> riverStates = ImmutableMap.of();

    @Inject
    public RiverStateService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        RiverStatesMetaData oldMetadata = event.previousState().getMetaData().custom(RiverStatesMetaData.TYPE);
        RiverStatesMetaData newMetadata = event.state().getMetaData().custom(RiverStatesMetaData.TYPE);
        if ((oldMetadata == null && newMetadata == null) || (oldMetadata != null && oldMetadata.equals(newMetadata))) {
            return;
        }
        Map<String, RiverState> survivors = newHashMap();
        for (Map.Entry<String, RiverState> entry : riverStates.entrySet()) {
            if (newMetadata != null && newMetadata.river(entry.getKey()) != null) {
                survivors.put(entry.getKey(), entry.getValue());
            }
        }
        ImmutableMap.Builder<String, RiverState> builder = ImmutableMap.builder();
        if (newMetadata != null) {
            for (RiverState riverMetaData : newMetadata.rivers()) {
                RiverState riverState = survivors.get(riverMetaData.getName());
                if (riverState != null) {
                    if (!riverState.getType().equals(riverMetaData.getType()) || !riverState.getSettings().equals(riverMetaData.getSettings())) {
                        logger.debug("updating river state [{}]", riverMetaData.getName());
                        riverState = riverMetaData;
                    }
                } else {
                    riverState = riverMetaData;
                }
                logger.debug("registering river state [{}]", riverMetaData.getName());
                builder.put(riverMetaData.getName(), riverState);
            }
        }
    }

    /**
     * Register a new river for river state management
     * @param request a river state register request
     * @param listener listener for cluster state update response
     */
    public void registerRiver(final RegisterRiverStateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        final RiverState newRiverMetaData = request.riverState;
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (!registerRiver(newRiverMetaData)) {
                    return currentState;
                }
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                RiverStatesMetaData rivers = currentState.metaData().custom(RiverStatesMetaData.TYPE);
                if (rivers == null) {
                    logger.info("put river state [{}]", request.name);
                    rivers = new RiverStatesMetaData(request.riverState);
                } else {
                    boolean found = false;
                    List<RiverState> riversMetaData = newLinkedList();
                    for (RiverState riverMetaData : rivers.rivers()) {
                        if (riverMetaData.getName().equals(newRiverMetaData.getName())) {
                            found = true;
                            riversMetaData.add(newRiverMetaData);
                        } else {
                            riversMetaData.add(riverMetaData);
                        }
                    }
                    if (!found) {
                        logger.info("put river state [{}]", request.name);
                        riversMetaData.add(request.riverState);
                    } else {
                        logger.info("update river state [{}]", request.name);
                    }
                    rivers = new RiverStatesMetaData(riversMetaData.toArray(new RiverState[riversMetaData.size()]));
                }
                metadataBuilder.putCustom(RiverStatesMetaData.TYPE, rivers);
                return ClusterState.builder(currentState).metaData(metadataBuilder).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failed to create river state [{}]", t, request.name);
                super.onFailure(source, t);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }

    /**
     * Unregister river from river state management
     * @param request the unregister river state request
     * @param listener listener for cluster state updates
     */
    public void unregisterRiver(final UnregisterRiverStateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(request.cause, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                RiverStatesMetaData rivers = currentState.metaData().custom(RiverStatesMetaData.TYPE);
                if (rivers != null && rivers.rivers().size() > 0) {
                    List<RiverState> riverStates = newLinkedList();
                    boolean changed = false;
                    for (RiverState riverState : rivers.rivers()) {
                        if (Regex.simpleMatch(request.name, riverState.getName())) {
                            logger.info("delete river state [{}]", riverState.getName());
                            changed = true;
                        } else {
                            riverStates.add(riverState);
                        }
                    }
                    if (changed) {
                        rivers = new RiverStatesMetaData(riverStates.toArray(new RiverState[riverStates.size()]));
                        metadataBuilder.putCustom(RiverStatesMetaData.TYPE, rivers);
                        return ClusterState.builder(currentState).metaData(metadataBuilder).build();
                    }
                }
                throw new ElasticsearchException("river state missing: " + request.name);
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return discoveryNode.masterNode();
            }
        });
    }

    private boolean registerRiver(RiverState riverMetaData) {
        RiverState previous = riverStates.get(riverMetaData.getName());
        if (previous != null) {
            if (!previous.getType().equals(riverMetaData.getType()) && previous.getSettings().equals(riverMetaData.getSettings())) {
                return false;
            }
        }
        Map<String, RiverState> newRiverStates = newHashMap();
        newRiverStates.put(riverMetaData.getName(), riverMetaData);
        riverStates = ImmutableMap.copyOf(newRiverStates);
        return true;
    }


    public static class RegisterRiverStateRequest extends ClusterStateUpdateRequest<RegisterRiverStateRequest> {

        final String cause;

        final String name;

        final String type;

        RiverState riverState;

        /**
         * Constructs new register river request
         *
         * @param cause river registration cause
         * @param name  river name
         * @param type  river type
         */
        public RegisterRiverStateRequest(String cause, String name, String type) {
            this.cause = cause;
            this.name = name;
            this.type = type;
        }

        /**
         * Sets river state
         *
         * @param riverState river state
         * @return this request
         */
        public RegisterRiverStateRequest riverState(RiverState riverState) {
            this.riverState = riverState;
            return this;
        }
    }

    public static class UnregisterRiverStateRequest extends ClusterStateUpdateRequest<UnregisterRiverStateRequest> {

        final String cause;

        final String name;

        /**
         * Creates a new unregister river state request
         *
         * @param cause river unregistration cause
         * @param name  river name
         */
        public UnregisterRiverStateRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }

    }
}
