
package com.buildyourownkafka.broker;

/**
 * Centralized network configuration for broker connections.
 */
public final class BrokerNetworkConfig {

    /**
     * Maximum read interval while receiving a frame.
     */
    public static final int SOCKET_READ_TIMEOUT_MS = 30_000;

    /**
     * Maximum duration allowed to receive one complete frame,
     * measured from the arrival of its first byte.
     */
    public static final int REQUEST_TIMEOUT_MS = 60_000;

    /**
     * Maximum number of concurrently active client connections.
     */
    public static final int MAX_CONNECTIONS = 100;

    private BrokerNetworkConfig() {
    }
}
