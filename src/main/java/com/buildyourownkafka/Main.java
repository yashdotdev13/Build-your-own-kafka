package com.buildyourownkafka;

import com.buildyourownkafka.broker.BrokerServer;

public class Main {

    public static void main(String[] args) throws Exception {

        BrokerServer brokerServer = new BrokerServer(9092);
        Runtime.getRuntime().addShutdownHook(new Thread(brokerServer::stop));
        brokerServer.start();
    }
}