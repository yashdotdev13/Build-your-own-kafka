
package com.buildyourownkafka.broker;

/**
 * Centralized network configuration for broker connections.
 */
public final class BrokerNetworkConfig {

    /**
     * Maximum time a socket read may remain idle,
     * in milliseconds.
     */
    public static final int SOCKET_READ_TIMEOUT_MS = 30_000;

    private BrokerNetworkConfig() {
    }
}
