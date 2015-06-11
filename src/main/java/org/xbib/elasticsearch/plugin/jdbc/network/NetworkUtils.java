package org.xbib.elasticsearch.plugin.jdbc.network;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;

public abstract class NetworkUtils {

    public static enum ProtocolVersion {
        IPv4, IPv6, IPv46, NONE
    }

    public static final String IPv4_SETTING = "java.net.preferIPv4Stack";
    public static final String IPv6_SETTING = "java.net.preferIPv6Addresses";

    private final static InetAddress localAddress;

    static {
        InetAddress address;
        try {
            address = InetAddress.getLocalHost();
        } catch (Throwable e) {
            address = InetAddress.getLoopbackAddress();
        }
        localAddress = address;
    }

    private NetworkUtils() {
    }

    public static InetAddress getLocalAddress() {
        return localAddress;
    }

    public static InetAddress getFirstNonLoopbackAddress(ProtocolVersion ip_version) throws SocketException {
        InetAddress address;
        for (NetworkInterface networkInterface : getNetworkInterfaces()) {
            try {
                if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                    continue;
                }
            } catch (Exception e) {
                continue;
            }
            address = getFirstNonLoopbackAddress(networkInterface, ip_version);
            if (address != null) {
                return address;
            }
        }
        return null;
    }

    public static InetAddress getFirstNonLoopbackAddress(NetworkInterface networkInterface, ProtocolVersion ipVersion) throws SocketException {
        if (networkInterface == null) {
            throw new IllegalArgumentException("network interface is null");
        }

        for (Enumeration addresses = networkInterface.getInetAddresses(); addresses.hasMoreElements(); ) {
            InetAddress address = (InetAddress) addresses.nextElement();
            if (!address.isLoopbackAddress()) {
                if ((address instanceof Inet4Address && ipVersion == ProtocolVersion.IPv4) ||
                        (address instanceof Inet6Address && ipVersion == ProtocolVersion.IPv6)) {
                    return address;
                }
            }
        }
        return null;
    }

    public static InetAddress getFirstAddress(NetworkInterface networkInterface, ProtocolVersion ipVersion) throws SocketException {
        if (networkInterface == null) {
            throw new IllegalArgumentException("network interface is null");
        }

        for (Enumeration addresses = networkInterface.getInetAddresses(); addresses.hasMoreElements(); ) {
            InetAddress address = (InetAddress) addresses.nextElement();
            if ((address instanceof Inet4Address && ipVersion == ProtocolVersion.IPv4) ||
                    (address instanceof Inet6Address && ipVersion == ProtocolVersion.IPv6)) {
                return address;
            }
        }
        return null;
    }

    public static List<NetworkInterface> getAllAvailableInterfaces() throws SocketException {
        List<NetworkInterface> allInterfaces = new ArrayList<>();
        for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements(); ) {
            NetworkInterface networkInterface = interfaces.nextElement();
            allInterfaces.add(networkInterface);
            Enumeration<NetworkInterface> subInterfaces = networkInterface.getSubInterfaces();
            if (subInterfaces.hasMoreElements()) {
                while (subInterfaces.hasMoreElements()) {
                    allInterfaces.add(subInterfaces.nextElement());
                }
            }
        }
        sortInterfaces(allInterfaces);
        return allInterfaces;
    }

    public static List<InetAddress> getAllAvailableAddresses() throws SocketException {
        List<InetAddress> allAddresses = new ArrayList<>();
        for (NetworkInterface networkInterface : getNetworkInterfaces()) {
            Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
            while (addrs.hasMoreElements()) {
                allAddresses.add(addrs.nextElement());
            }
        }
        sortAddresses(allAddresses);
        return allAddresses;
    }

    public static ProtocolVersion getProtocolVersion() throws SocketException {
        switch (findAvailableProtocols()) {
            case IPv4:
                return ProtocolVersion.IPv4;
            case IPv6:
                return ProtocolVersion.IPv6;
            case IPv46:
                if (Boolean.getBoolean(System.getProperty(IPv4_SETTING))) {
                    return ProtocolVersion.IPv4;
                }
                if (Boolean.getBoolean(System.getProperty(IPv6_SETTING))) {
                    return ProtocolVersion.IPv6;
                }
                return ProtocolVersion.IPv6;
        }
        return ProtocolVersion.NONE;
    }

    public static ProtocolVersion findAvailableProtocols() throws SocketException {
        boolean hasIPv4 = false;
        boolean hasIPv6 = false;
        for (InetAddress addr : getAllAvailableAddresses()) {
            if (addr instanceof Inet4Address) {
                hasIPv4 = true;
            }
            if (addr instanceof Inet6Address) {
                hasIPv6 = true;
            }
        }
        if (hasIPv4 && hasIPv6) {
            return ProtocolVersion.IPv46;
        }
        if (hasIPv4) {
            return ProtocolVersion.IPv4;
        }
        if (hasIPv6) {
            return ProtocolVersion.IPv6;
        }
        return ProtocolVersion.NONE;
    }

    public static InetAddress resolveInetAddress(String host, String defaultValue) throws IOException {
        if (host == null) {
            host = defaultValue;
        }
        String origHost = host;
        int pos = host.indexOf(':');
        if (pos > 0) {
            host = host.substring(0, pos - 1);
        }
        if ((host.startsWith("#") && host.endsWith("#")) || (host.startsWith("_") && host.endsWith("_"))) {
            host = host.substring(1, host.length() - 1);
            if (host.equals("local")) {
                return getLocalAddress();
            } else if (host.startsWith("non_loopback")) {
                if (host.toLowerCase(Locale.ROOT).endsWith(":ipv4")) {
                    return getFirstNonLoopbackAddress(ProtocolVersion.IPv4);
                } else if (host.toLowerCase(Locale.ROOT).endsWith(":ipv6")) {
                    return getFirstNonLoopbackAddress(ProtocolVersion.IPv6);
                } else {
                    return getFirstNonLoopbackAddress(getProtocolVersion());
                }
            } else {
                ProtocolVersion protocolVersion = getProtocolVersion();
                if (host.toLowerCase(Locale.ROOT).endsWith(":ipv4")) {
                    protocolVersion = ProtocolVersion.IPv4;
                    host = host.substring(0, host.length() - 5);
                } else if (host.toLowerCase(Locale.ROOT).endsWith(":ipv6")) {
                    protocolVersion = ProtocolVersion.IPv6;
                    host = host.substring(0, host.length() - 5);
                }
                for (NetworkInterface ni : getAllAvailableInterfaces()) {
                    if (!ni.isUp()) {
                        continue;
                    }
                    if (host.equals(ni.getName()) || host.equals(ni.getDisplayName())) {
                        if (ni.isLoopback()) {
                            return getFirstAddress(ni, protocolVersion);
                        } else {
                            return getFirstNonLoopbackAddress(ni, protocolVersion);
                        }
                    }
                }
            }
            throw new IOException("failed to find network interface for [" + origHost + "]");
        }
        return InetAddress.getByName(host);
    }

    private static List<NetworkInterface> getNetworkInterfaces() throws SocketException {
        List<NetworkInterface> networkInterfaces = new ArrayList<>();
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            networkInterfaces.add(networkInterface);
            Enumeration<NetworkInterface> subInterfaces = networkInterface.getSubInterfaces();
            if (subInterfaces.hasMoreElements()) {
                while (subInterfaces.hasMoreElements()) {
                    networkInterfaces.add(subInterfaces.nextElement());
                }
            }
        }
        sortInterfaces(networkInterfaces);
        return networkInterfaces;
    }

    private static void sortInterfaces(List<NetworkInterface> interfaces) {
        Collections.sort(interfaces, new Comparator<NetworkInterface>() {
            @Override
            public int compare(NetworkInterface o1, NetworkInterface o2) {
                return Integer.compare(o1.getIndex(), o2.getIndex());
            }
        });
    }

    private static void sortAddresses(List<InetAddress> addressList) {
        Collections.sort(addressList, new Comparator<InetAddress>() {
            @Override
            public int compare(InetAddress o1, InetAddress o2) {
                return compareBytes(o1.getAddress(), o2.getAddress());
            }
        });
    }

    private static int compareBytes(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}
