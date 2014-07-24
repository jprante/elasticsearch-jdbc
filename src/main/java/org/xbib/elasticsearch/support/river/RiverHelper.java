package org.xbib.elasticsearch.support.river;

import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiversService;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverState;
import org.xbib.elasticsearch.action.river.jdbc.state.get.GetRiverStateAction;
import org.xbib.elasticsearch.action.river.jdbc.state.get.GetRiverStateRequest;
import org.xbib.elasticsearch.action.river.jdbc.state.get.GetRiverStateResponse;

import java.io.IOException;
import java.lang.reflect.Field;

public class RiverHelper {

    private RiverHelper() {
    }

    public static void waitForRiverEnabled(ClusterAdminClient client, String riverName, String riverType) throws InterruptedException, IOException {
        waitForRiverEnabled(client, riverName, riverType, 15);
    }

    public static void waitForRiverEnabled(ClusterAdminClient client, String riverName, String riverType, int seconds) throws InterruptedException, IOException {
        GetRiverStateRequest riverStateRequest = new GetRiverStateRequest()
                .setRiverName(riverName)
                .setRiverType(riverType);
        GetRiverStateResponse riverStateResponse = client
                .execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
        while (seconds-- > 0 && !isEnabled(riverName, riverStateResponse)) {
            Thread.sleep(1000L);
            try {
                riverStateResponse = client.execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
            } catch (IndexMissingException e) {
                // ignore
            }
        }
    }

    private static boolean isEnabled(String riverName, GetRiverStateResponse riverStateResponse) {
        if (riverStateResponse == null) {
            return false;
        }
        if (riverStateResponse.getStates() == null) {
            return false;
        }
        if (riverStateResponse.getStates().isEmpty()) {
            return false;
        }
        for (RiverState state : riverStateResponse.getStates()) {
            if (state.getName().equals(riverName)) {
                return state.isEnabled();
            }
        }
        return false;
    }

    public static void waitForActiveRiver(ClusterAdminClient client, String riverName, String riverType) throws InterruptedException, IOException {
        waitForInactiveRiver(client, riverName, riverType, 30);
    }

    public static void waitForActiveRiver(ClusterAdminClient client, String riverName, String riverType, int seconds) throws InterruptedException, IOException {
        GetRiverStateRequest riverStateRequest = new GetRiverStateRequest()
                .setRiverName(riverName)
                .setRiverType(riverType);
        GetRiverStateResponse riverStateResponse = client
                .execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
        while (seconds-- > 0 && !isActive(riverName, riverStateResponse)) {
            Thread.sleep(1000L);
            try {
                riverStateResponse = client.execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
            } catch (IndexMissingException e) {
                //
            }
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for active river");
        }
    }

    public static void waitForInactiveRiver(ClusterAdminClient client, String riverName, String riverType) throws InterruptedException, IOException {
        waitForInactiveRiver(client, riverName, riverType, 30);
    }

    public static void waitForInactiveRiver(ClusterAdminClient client, String riverName, String riverType, int seconds) throws InterruptedException, IOException {
        GetRiverStateRequest riverStateRequest = new GetRiverStateRequest()
                .setRiverName(riverName)
                .setRiverType(riverType);
        GetRiverStateResponse riverStateResponse = client
                .execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
        while (seconds-- > 0 && isActive(riverName, riverStateResponse)) {
            Thread.sleep(1000L);
            try {
                riverStateResponse = client.execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
            } catch (IndexMissingException e) {
                //
            }
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for inactive river");
        }
    }

    private static Boolean isActive(String riverName, GetRiverStateResponse riverStateResponse) {
        if (riverStateResponse == null) {
            return false;
        }
        if (riverStateResponse.getStates() == null) {
            return false;
        }
        if (riverStateResponse.getStates().isEmpty()) {
            return false;
        }
        for (RiverState state : riverStateResponse.getStates()) {
            if (state != null && state.getName().equals(riverName)) {
                return state.isActive() != null && state.isActive();
            }
        }
        return false;
    }

    /**
     * Retrieve the registered rivers using reflection. This hack can be removed when RiversService gets a public API.
     *
     * @param injector injector
     * @return map of rivers or null if not possible
     */
    @SuppressWarnings({"unchecked"})
    public static ImmutableMap<RiverName, River> rivers(Injector injector) {
        RiversService riversService = injector.getInstance(RiversService.class);
        try {
            Field field = RiversService.class.getDeclaredField("rivers");
            if (field != null) {
                field.setAccessible(true);
                return (ImmutableMap<RiverName, River>) field.get(riversService);
            }
        } catch (Throwable e) {
            //
        }
        return ImmutableMap.of();
    }


}
