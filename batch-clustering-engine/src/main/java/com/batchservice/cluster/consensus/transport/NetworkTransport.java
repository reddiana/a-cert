package com.batchservice.cluster.consensus.transport;

import java.util.function.Consumer;

public interface NetworkTransport {
    void send(Message message);
    void registerReceiver(String nodeId, Consumer<Message> receiver);
    void unregisterReceiver(String nodeId);
}
